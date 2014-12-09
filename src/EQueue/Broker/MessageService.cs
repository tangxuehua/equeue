using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Scheduling;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class MessageService : IMessageService
    {
        private readonly ConcurrentDictionary<string, IList<Queue>> _topicQueueDict = new ConcurrentDictionary<string, IList<Queue>>();
        private readonly IMessageStore _messageStore;
        private readonly IOffsetManager _offsetManager;
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;
        private BrokerController _brokerController;
        private int _removeConsumedQueueIndexTaskId;
        private int _removeExceedMaxCacheQueueIndexTaskId;
        private long _totalRecoveredQueueIndex;
        private int _isRemovingConsumedQueueIndex;

        public MessageService(IMessageStore messageStore, IOffsetManager offsetManager, IScheduleService scheduleService)
        {
            _messageStore = messageStore;
            _offsetManager = offsetManager;
            _scheduleService = scheduleService;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void SetBrokerContrller(BrokerController brokerController)
        {
            _brokerController = brokerController;
        }
        public void Start()
        {
            Clear();
            _offsetManager.Recover();
            _messageStore.Recover(RecoverQueueIndexForMessage);
            _messageStore.Start();
            _offsetManager.Start();
            _removeConsumedQueueIndexTaskId = _scheduleService.ScheduleTask("MessageService.RemoveConsumedQueueIndex", RemoveConsumedQueueIndex, _brokerController.Setting.RemoveConsumedMessageInterval, _brokerController.Setting.RemoveConsumedMessageInterval);
            _removeExceedMaxCacheQueueIndexTaskId = _scheduleService.ScheduleTask("MessageService.RemoveExceedMaxCacheQueueIndex", RemoveExceedMaxCacheQueueIndex, _brokerController.Setting.RemoveExceedMaxCacheQueueIndexInterval, _brokerController.Setting.RemoveExceedMaxCacheQueueIndexInterval);
        }
        public void Shutdown()
        {
            _messageStore.Shutdown();
            _offsetManager.Shutdown();
            _scheduleService.ShutdownTask(_removeConsumedQueueIndexTaskId);
            _scheduleService.ShutdownTask(_removeExceedMaxCacheQueueIndexTaskId);
        }
        public MessageStoreResult StoreMessage(Message message, int queueId, string routingKey)
        {
            var queues = GetQueues(message.Topic);
            if (queues.Count == 0)
            {
                throw new Exception(string.Format("No available queue for storing message. topic:{0}", message.Topic));
            }
            var queue = queues.SingleOrDefault(x => x.QueueId == queueId);
            if (queue == null)
            {
                throw new InvalidQueueIdException(message.Topic, queues.Select(x => x.QueueId), queueId);
            }
            var queueOffset = queue.IncrementCurrentOffset();
            var queueMessage = _messageStore.StoreMessage(queueId, queueOffset, message, routingKey);
            queue.SetQueueIndex(queueMessage.QueueOffset, queueMessage.MessageOffset);
            return new MessageStoreResult(queueMessage.MessageOffset, queueMessage.QueueId, queueMessage.QueueOffset);
        }
        public IEnumerable<QueueMessage> GetMessages(string topic, int queueId, long queueOffset, int batchSize)
        {
            var queues = GetQueues(topic);
            var queue = queues.SingleOrDefault(x => x.QueueId == queueId);
            if (queue != null)
            {
                var messages = new List<QueueMessage>();
                var maxQueueOffset = queueOffset + batchSize > queue.CurrentOffset ? queue.CurrentOffset + 1 : queueOffset + batchSize;
                for (var currentQueueOffset = queueOffset; currentQueueOffset < maxQueueOffset; currentQueueOffset++)
                {
                    var messageOffset = queue.GetMessageOffset(currentQueueOffset);
                    if (messageOffset >= 0)
                    {
                        var message = _messageStore.GetMessage(messageOffset);
                        if (message != null)
                        {
                            messages.Add(message);
                        }
                        else
                        {
                            _logger.ErrorFormat("Cannot find the message by messageOffset, please check if the message exist. topic:{0}, queueId:{1}, queueOffset:{2}, messageOffset:{3}", topic, queueId, currentQueueOffset, messageOffset);
                        }
                    }
                    else
                    {
                        if (currentQueueOffset < queue.CurrentOffset && _messageStore.SupportBatchLoadQueueIndex)
                        {
                            //Batch load queue index from message store.
                            var indexDict = _messageStore.BatchLoadQueueIndex(topic, queueId, currentQueueOffset);
                            foreach (var entry in indexDict)
                            {
                                queue.SetQueueIndex(entry.Key, entry.Value);
                            }

                            //Get message offset again from queue.
                            messageOffset = queue.GetMessageOffset(currentQueueOffset);
                            if (messageOffset >= 0)
                            {
                                var message = _messageStore.GetMessage(messageOffset);
                                if (message != null)
                                {
                                    messages.Add(message);
                                }
                                else
                                {
                                    _logger.ErrorFormat("Cannot find the message by messageOffset after batch loading queue index, please check if the message exist. topic:{0}, queueId:{1}, queueOffset:{2}, messageOffset:{3}", topic, queueId, currentQueueOffset, messageOffset);
                                }
                            }
                            else
                            {
                                _logger.ErrorFormat("Cannot find the messageOffset by queueOffset, please check if the message exist. topic:{0}, queueId:{1}, queueOffset:{0}", topic, queueId, currentQueueOffset);
                            }
                        }
                    }
                }
                return messages;
            }
            return new QueueMessage[0];
        }
        public long GetQueueCurrentOffset(string topic, int queueId)
        {
            var queues = GetQueues(topic);
            var queue = queues.SingleOrDefault(x => x.QueueId == queueId);
            if (queue != null)
            {
                return queue.CurrentOffset;
            }
            return -1;
        }
        public long GetQueueMinOffset(string topic, int queueId)
        {
            var queues = GetQueues(topic);
            var queue = queues.SingleOrDefault(x => x.QueueId == queueId);
            if (queue != null)
            {
                var offset = queue.GetMinQueueOffset();
                return offset != null ? offset.Value : -1;
            }
            return -1;
        }

        public IEnumerable<string> GetAllTopics()
        {
            return _topicQueueDict.Keys;
        }
        public IEnumerable<int> GetQueueIdsForProducer(string topic)
        {
            return GetQueues(topic).Where(x => x.Status == QueueStatus.Normal).Select(x => x.QueueId);
        }
        public IEnumerable<int> GetQueueIdsForConsumer(string topic)
        {
            return GetQueues(topic).Select(x => x.QueueId);
        }
        public IList<Queue> QueryQueues(string topic)
        {
            var queuesList = _topicQueueDict.Where(x => x.Key.Contains(topic)).Select(x => x.Value);
            var totalQueus = new List<Queue>();
            queuesList.ForEach(x => x.ForEach(y => totalQueus.Add(y)));
            return totalQueus;
        }
        public void AddQueue(string topic)
        {
            var queues = GetQueues(topic);
            if (queues.Count == 0)
            {
                queues.Add(new Queue(topic, 0));
            }
            else
            {
                queues.Add(new Queue(topic, queues.Max(x => x.QueueId) + 1));
            }
        }
        public void RemoveQueue(string topic, int queueId)
        {
            var queues = GetQueues(topic);
            var queue = queues.SingleOrDefault(x => x.QueueId == queueId);
            if (queue != null)
            {
                queues.Remove(queue);
                _offsetManager.RemoveQueueOffset(topic, queueId);
                _messageStore.UpdateMaxAllowToDeleteQueueOffset(topic, queueId, long.MaxValue);
            }
        }
        public void EnableQueue(string topic, int queueId)
        {
            var queue = GetQueues(topic).SingleOrDefault(x => x.QueueId == queueId);
            if (queue != null)
            {
                queue.Enable();
            }
        }
        public void DisableQueue(string topic, int queueId)
        {
            var queue = GetQueues(topic).SingleOrDefault(x => x.QueueId == queueId);
            if (queue != null)
            {
                queue.Disable();
            }
        }
        public IEnumerable<QueueMessage> QueryMessages(string topic, int? queueId, int? code, string routingKey, int pageIndex, int pageSize, out int total)
        {
            return _messageStore.QueryMessages(topic, queueId, code, routingKey, pageIndex, pageSize, out total);
        }
        public QueueMessage GetMessageDetail(long messageOffset)
        {
            return _messageStore.GetMessage(messageOffset);
        }

        private void Clear()
        {
            _topicQueueDict.Clear();
        }
        private IList<Queue> GetQueues(string topic)
        {
            return _topicQueueDict.GetOrAdd(topic, x =>
            {
                var queues = new List<Queue>();
                for (var index = 0; index < _brokerController.Setting.DefaultTopicQueueCount; index++)
                {
                    queues.Add(new Queue(x, index));
                }
                return queues;
            });
        }
        private void RecoverQueueIndexForMessage(long messageOffset, string topic, int queueId, long queueOffset)
        {
            var queues = GetQueues(topic);
            if (queueId >= queues.Count)
            {
                for (var index = queues.Count; index <= queueId; index++)
                {
                    queues.Add(new Queue(topic, index));
                }
            }
            var queue = queues[queueId];
            var allowSetQueueIndex = !_messageStore.SupportBatchLoadQueueIndex || _totalRecoveredQueueIndex < _brokerController.Setting.QueueIndexMaxCacheSize;
            queue.RecoverQueueIndex(queueOffset, messageOffset, allowSetQueueIndex);
            _totalRecoveredQueueIndex++;
        }
        private void RemoveConsumedQueueIndex()
        {
            if (Interlocked.CompareExchange(ref _isRemovingConsumedQueueIndex, 1, 0) == 0)
            {
                try
                {
                    var totalRemovedCount = 0L;
                    foreach (var topicQueues in _topicQueueDict.Values)
                    {
                        foreach (var queue in topicQueues)
                        {
                            var consumedQueueOffset = _offsetManager.GetMinOffset(queue.Topic, queue.QueueId);
                            if (consumedQueueOffset > queue.CurrentOffset)
                            {
                                consumedQueueOffset = queue.CurrentOffset;
                            }
                            totalRemovedCount += queue.RemoveConsumedQueueIndex(consumedQueueOffset);
                            _messageStore.UpdateMaxAllowToDeleteQueueOffset(queue.Topic, queue.QueueId, consumedQueueOffset);
                        }
                    }
                    if (totalRemovedCount > 0)
                    {
                        _logger.InfoFormat("Auto removed {0} consumed queue index from memory.", totalRemovedCount);
                    }
                }
                catch (Exception ex)
                {
                    _logger.Error("Failed to remove consumed queue index.", ex);
                }
                finally
                {
                    Interlocked.Exchange(ref _isRemovingConsumedQueueIndex, 0);
                }
            }
        }
        private long GetAllQueueIndexCount()
        {
            var totalQueueIndexCount = 0L;
            foreach (var queues in _topicQueueDict.Values)
            {
                foreach (var queue in queues)
                {
                    totalQueueIndexCount += queue.GetMessageCount();
                }
            }
            return totalQueueIndexCount;
        }
        private void RemoveExceedMaxCacheQueueIndex()
        {
            if (!_messageStore.SupportBatchLoadQueueIndex)
            {
                return;
            }

            var exceedCount = GetAllQueueIndexCount() - _brokerController.Setting.QueueIndexMaxCacheSize;
            if (exceedCount > 0)
            {
                //First we should remove all the consumed queue index from memory.
                RemoveConsumedQueueIndex();

                var queueEntryList = new List<KeyValuePair<Queue, long>>();
                foreach (var queues in _topicQueueDict.Values)
                {
                    foreach (var queue in queues)
                    {
                        queueEntryList.Add(new KeyValuePair<Queue, long>(queue, queue.GetMessageCount()));
                    }
                }
                var totalUnConsumedQueueIndexCount = queueEntryList.Sum(x => x.Value);
                var unconsumedExceedCount = totalUnConsumedQueueIndexCount - _brokerController.Setting.QueueIndexMaxCacheSize;
                if (unconsumedExceedCount <= 0)
                {
                    return;
                }

                //If the remaining queue index count still exceed the max queue index cache size, then we try to remove all the exceeded unconsumed queue indexes.
                var totalRemovedCount = 0L;
                foreach (var entry in queueEntryList)
                {
                    var queueToRemoveCount = unconsumedExceedCount * entry.Value / totalUnConsumedQueueIndexCount;
                    if (queueToRemoveCount > 0)
                    {
                        totalRemovedCount += entry.Key.RemoveLastQueueIndex(queueToRemoveCount);
                    }
                }
                if (totalRemovedCount > 0)
                {
                    _logger.InfoFormat("Auto removed {0} consumed queue indexes which exceed the max cache size, current total unconsumed queue index count:{1}, current exceed count:{2}", totalRemovedCount, totalUnConsumedQueueIndexCount, exceedCount);
                }
            }
        }
    }
}
