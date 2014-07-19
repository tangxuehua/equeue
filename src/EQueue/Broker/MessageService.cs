using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Scheduling;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class MessageService : IMessageService
    {
        private ConcurrentDictionary<string, IList<Queue>> _topicQueueDict = new ConcurrentDictionary<string, IList<Queue>>();
        private readonly IMessageStore _messageStore;
        private readonly IOffsetManager _offsetManager;
        private readonly IScheduleService _scheduleService;
        private ILogger _logger;
        private BrokerController _brokerController;
        private int _removeConsumedMessagesTaskId;
        private int _removeQueueIndexTaskId;
        private long _totalRecoveredQueueIndex;

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
            _removeConsumedMessagesTaskId = _scheduleService.ScheduleTask("MessageService.RemoveConsumedMessages", RemoveConsumedMessages, _brokerController.Setting.RemoveMessageInterval, _brokerController.Setting.RemoveMessageInterval);
            _removeQueueIndexTaskId = _scheduleService.ScheduleTask("MessageService.RemoveQueueIndex", RemoveQueueIndex, _brokerController.Setting.RemoveQueueIndexInterval, _brokerController.Setting.RemoveQueueIndexInterval);
        }
        public void Shutdown()
        {
            _messageStore.Shutdown();
            _offsetManager.Shutdown();
            _scheduleService.ShutdownTask(_removeConsumedMessagesTaskId);
            _scheduleService.ShutdownTask(_removeQueueIndexTaskId);
        }
        public MessageStoreResult StoreMessage(Message message, int queueId)
        {
            var queues = GetQueues(message.Topic);
            var queueCount = queues.Count;
            if (queueId >= queueCount || queueId < 0)
            {
                throw new InvalidQueueIdException(message.Topic, queueCount, queueId);
            }
            var queue = queues[queueId];
            var queueOffset = queue.IncrementCurrentOffset();
            var queueMessage = _messageStore.StoreMessage(queueId, queueOffset, message);
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

        public IEnumerable<string> GetAllTopics()
        {
            return _topicQueueDict.Keys;
        }
        public int GetTopicQueueCount(string topic)
        {
            return GetQueues(topic).Count;
        }
        public IList<Queue> GetQueues(string topic)
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
        public IList<Queue> QueryQueues(string topic)
        {
            var queuesList = _topicQueueDict.Where(x => x.Key.Contains(topic)).Select(x => x.Value);
            var totalQueus = new List<Queue>();
            queuesList.ForEach(x => x.ForEach(y => totalQueus.Add(y)));
            return totalQueus;
        }

        private void Clear()
        {
            _topicQueueDict.Clear();
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
        private void RemoveConsumedMessages()
        {
            foreach (var topicQueues in _topicQueueDict.Values)
            {
                foreach (var queue in topicQueues)
                {
                    var consumedQueueOffset = _offsetManager.GetMinOffset(queue.Topic, queue.QueueId);
                    if (consumedQueueOffset > queue.CurrentOffset)
                    {
                        consumedQueueOffset = queue.CurrentOffset;
                    }
                    queue.RemoveQueueIndex(consumedQueueOffset);
                    _messageStore.UpdateMaxAllowToDeleteQueueOffset(queue.Topic, queue.QueueId, consumedQueueOffset);
                }
            }
        }
        private void RemoveQueueIndex()
        {
            if (!_messageStore.SupportBatchLoadQueueIndex)
            {
                return;
            }

            var queueEntryList = new List<KeyValuePair<Queue, long>>();
            foreach (var queues in _topicQueueDict.Values.ToList())
            {
                foreach (var queue in queues)
                {
                    queueEntryList.Add(new KeyValuePair<Queue, long>(queue, queue.GetMessageCount()));
                }
            }
            var totalQueueIndexCount = queueEntryList.Sum(x => x.Value);
            var exceedCount = totalQueueIndexCount - _brokerController.Setting.QueueIndexMaxCacheSize;
            if (exceedCount > 0)
            {
                var totalRemovedCount = 0L;
                foreach (var entry in queueEntryList)
                {
                    var queueToRemoveCount = exceedCount * entry.Value / totalQueueIndexCount;
                    if (queueToRemoveCount > 0)
                    {
                        totalRemovedCount += entry.Key.RemoveLastQueueIndex(queueToRemoveCount);
                    }
                }
                if (totalRemovedCount > 0)
                {
                    _logger.InfoFormat("Exceed queue index max cache size, exceed count:{0}, current total count:{1}, total removed count:{2}", exceedCount, totalQueueIndexCount, totalRemovedCount);
                }
            }
        }
    }
}
