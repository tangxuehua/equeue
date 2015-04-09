using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Utilities;
using EQueue.Broker.Client;

namespace EQueue.Broker
{
    public class QueueService : IQueueService
    {
        private readonly ConcurrentDictionary<string, Queue> _queueDict;
        private readonly IQueueStore _queueStore;
        private readonly IMessageStore _messageStore;
        private readonly IOffsetManager _offsetManager;
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;
        private readonly IList<int> _taskIds;
        private int _isRemovingConsumedQueueIndex;
        private int _isRemovingExceedMaxCacheQueueIndex;

        public QueueService(IQueueStore queueStore, IMessageStore messageStore, IOffsetManager offsetManager, IScheduleService scheduleService, ILoggerFactory loggerFactory)
        {
            _taskIds = new List<int>();
            _queueDict = new ConcurrentDictionary<string, Queue>();
            _queueStore = queueStore;
            _messageStore = messageStore;
            _offsetManager = offsetManager;
            _scheduleService = scheduleService;
            _logger = loggerFactory.Create(GetType().FullName);
        }

        public void Start()
        {
            //先清理状态
            _queueDict.Clear();
            foreach (var taskId in _taskIds)
            {
                _scheduleService.ShutdownTask(taskId);
            }

            //再重新加载状态
            var queues = _queueStore.LoadAllQueues();
            var queueGroups = queues.GroupBy(x => x.Topic);
            foreach (var queue in queues)
            {
                var key = CreateQueueKey(queue.Topic, queue.QueueId);
                _queueDict.TryAdd(key, queue);
            }
            _taskIds.Add(_scheduleService.ScheduleTask("QueueService.RemoveConsumedQueueIndex", RemoveConsumedQueueIndex, BrokerController.Instance.Setting.RemoveConsumedMessageInterval, BrokerController.Instance.Setting.RemoveConsumedMessageInterval));
            _taskIds.Add(_scheduleService.ScheduleTask("QueueService.RemoveExceedMaxCacheQueueIndex", RemoveExceedMaxCacheQueueIndex, BrokerController.Instance.Setting.RemoveExceedMaxCacheQueueIndexInterval, BrokerController.Instance.Setting.RemoveExceedMaxCacheQueueIndexInterval));
        }
        public void Shutdown()
        {
            foreach (var taskId in _taskIds)
            {
                _scheduleService.ShutdownTask(taskId);
            }
        }
        public IEnumerable<string> GetAllTopics()
        {
            return _queueDict.Values.Select(x => x.Topic).Distinct();
        }
        public int GetAllQueueCount()
        {
            return _queueDict.Count;
        }
        public long GetAllQueueIndexCount()
        {
            return _queueDict.Values.Sum(x => x.GetMessageCount());
        }
        public long GetAllQueueUnConusmedMessageCount()
        {
            return _queueDict.Values.Sum(x => x.GetMessageRealCount());
        }
        public long GetQueueMinMessageOffset()
        {
            var minMessageOffset = -1L;
            if (_queueDict.Count > 0)
            {
                minMessageOffset = _queueDict.Values.Min(x => x.GetMinQueueOffset());
            }
            return minMessageOffset;
        }
        public bool IsQueueExist(string topic, int queueId)
        {
            var key = CreateQueueKey(topic, queueId);
            return _queueDict.ContainsKey(key);
        }
        public long GetQueueCurrentOffset(string topic, int queueId)
        {
            var key = CreateQueueKey(topic, queueId);
            Queue queue;
            if (_queueDict.TryGetValue(key, out queue))
            {
                return queue.CurrentOffset;
            }
            return -1;
        }
        public long GetQueueMinOffset(string topic, int queueId)
        {
            var key = CreateQueueKey(topic, queueId);
            Queue queue;
            if (_queueDict.TryGetValue(key, out queue))
            {
                return queue.GetMinQueueOffset();
            }
            return -1;
        }
        public void CreateTopic(string topic, int initialQueueCount)
        {
            lock (this)
            {
                Ensure.NotNullOrEmpty(topic, "topic");
                Ensure.Positive(initialQueueCount, "initialQueueCount");
                if (initialQueueCount > BrokerController.Instance.Setting.TopicMaxQueueCount)
                {
                    throw new ArgumentException(string.Format("Initial queue count cannot bigger than {0}.", BrokerController.Instance.Setting.TopicMaxQueueCount));
                }
                if (_queueDict.Values.Any(x => x.Topic == topic))
                {
                    throw new ArgumentException("Topic '{0}' already exist.");
                }

                var queues = new List<Queue>();
                for (var index = 0; index < initialQueueCount; index++)
                {
                    queues.Add(new Queue(topic, index));
                }
                _queueStore.CreateQueues(queues);
                foreach (var queue in queues)
                {
                    var key = CreateQueueKey(queue.Topic, queue.QueueId);
                    _queueDict.TryAdd(key, queue);
                }
            }
        }
        public Queue GetQueue(string topic, int queueId)
        {
            var key = CreateQueueKey(topic, queueId);
            Queue queue;
            if (_queueDict.TryGetValue(key, out queue))
            {
                return queue;
            }
            return null;
        }
        public void AddQueue(string topic)
        {
            Ensure.NotNullOrEmpty(topic, "topic");
            var queues = _queueDict.Values.Where(x => x.Topic == topic);
            if (queues.Count() == BrokerController.Instance.Setting.TopicMaxQueueCount)
            {
                throw new ArgumentException(string.Format("Queue count cannot bigger than {0}.", BrokerController.Instance.Setting.TopicMaxQueueCount));
            }
            var queueId = queues.Count() == 0 ? 0 : queues.Max(x => x.QueueId) + 1;
            var queue = new Queue(topic, queueId);
            _queueStore.CreateQueues(new Queue[] { queue });
            var key = CreateQueueKey(queue.Topic, queue.QueueId);
            _queueDict.TryAdd(key, queue);
        }
        public void RemoveQueue(string topic, int queueId)
        {
            var key = CreateQueueKey(topic, queueId);
            Queue queue;
            if (!_queueDict.TryGetValue(key, out queue))
            {
                return;
            }

            //检查队列状态是否是已禁用
            if (queue.Status != QueueStatus.Disabled)
            {
                throw new Exception("Queue status is not disabled, cannot be deleted.");
            }
            //检查是否有未消费完的消息
            if (queue.GetMessageRealCount() > 0L)
            {
                throw new Exception("Queue is not allowed to delete as there are messages exist in this queue.");
            }

            //删除队列消息
            _messageStore.DeleteQueueMessage(topic, queueId);

            //删除队列消费进度信息
            _offsetManager.DeleteQueueOffset(topic, queueId);

            //删除队列
            _queueStore.DeleteQueue(queue);

            //从内存移除队列
            _queueDict.Remove(key);
        }
        public void EnableQueue(string topic, int queueId)
        {
            var queue = GetQueue(topic, queueId);
            if (queue != null)
            {
                var foundQueue = _queueStore.GetQueue(topic, queueId);
                if (foundQueue!= null)
                {
                    foundQueue.Enable();
                    _queueStore.UpdateQueue(foundQueue);
                    queue.Enable();
                }
            }
        }
        public void DisableQueue(string topic, int queueId)
        {
            var queue = GetQueue(topic, queueId);
            if (queue != null)
            {
                var foundQueue = _queueStore.GetQueue(topic, queueId);
                if (foundQueue != null)
                {
                    foundQueue.Disable();
                    _queueStore.UpdateQueue(foundQueue);
                    queue.Disable();
                }
            }
        }
        public IEnumerable<Queue> QueryQueues(string topic)
        {
            return _queueDict.Values.Where(x => x.Topic.Contains(topic));
        }
        public IEnumerable<Queue> FindQueues(string topic, QueueStatus? status = null)
        {
            var queues = _queueDict.Values.Where(x => x.Topic == topic);
            if (status != null)
            {
                return queues.Where(x => x.Status == status.Value);
            }
            return queues;
        }

        private static string CreateQueueKey(string topic, int queueId)
        {
            return string.Format("{0}-{1}", topic, queueId);
        }
        private void RemoveConsumedQueueIndex()
        {
            if (Interlocked.CompareExchange(ref _isRemovingConsumedQueueIndex, 1, 0) == 0)
            {
                try
                {
                    foreach (var queue in _queueDict.Values)
                    {
                        var consumedQueueOffset = _offsetManager.GetMinOffset(queue.Topic, queue.QueueId);
                        if (consumedQueueOffset > queue.CurrentOffset)
                        {
                            consumedQueueOffset = queue.CurrentOffset;
                        }
                        queue.RemoveConsumedQueueIndex(consumedQueueOffset);
                        _messageStore.UpdateConsumedQueueOffset(queue.Topic, queue.QueueId, consumedQueueOffset);
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
        private void RemoveExceedMaxCacheQueueIndex()
        {
            if (Interlocked.CompareExchange(ref _isRemovingExceedMaxCacheQueueIndex, 1, 0) == 0)
            {
                try
                {
                    if (!_messageStore.SupportBatchLoadQueueIndex)
                    {
                        return;
                    }

                    var exceedCount = GetAllQueueIndexCount() - BrokerController.Instance.Setting.QueueIndexMaxCacheSize;
                    if (exceedCount > 0)
                    {
                        //First we should remove all the consumed queue index from memory.
                        RemoveConsumedQueueIndex();

                        var queueEntryList = new List<KeyValuePair<Queue, long>>();
                        foreach (var queue in _queueDict.Values)
                        {
                            queueEntryList.Add(new KeyValuePair<Queue, long>(queue, queue.GetMessageCount()));
                        }
                        var totalUnConsumedQueueIndexCount = queueEntryList.Sum(x => x.Value);
                        var unconsumedExceedCount = totalUnConsumedQueueIndexCount - BrokerController.Instance.Setting.QueueIndexMaxCacheSize;
                        if (unconsumedExceedCount <= 0)
                        {
                            return;
                        }

                        //If the remaining queue index count still exceed the max queue index cache size, then we try to remove all the exceeded unconsumed queue indexes.
                        var totalRemovedCount = 0L;
                        foreach (var entry in queueEntryList)
                        {
                            var requireRemoveCount = unconsumedExceedCount * entry.Value / totalUnConsumedQueueIndexCount;
                            if (requireRemoveCount > 0)
                            {
                                totalRemovedCount += entry.Key.RemoveRequiredQueueIndexFromLast(requireRemoveCount);
                            }
                        }
                        if (totalRemovedCount > 0)
                        {
                            _logger.InfoFormat("Auto removed {0} unconsumed queue indexes which exceed the max queue cache size, current total unconsumed queue index count:{1}, current exceed count:{2}", totalRemovedCount, totalUnConsumedQueueIndexCount, exceedCount);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.Error("Failed to remove exceed max cache queue index.", ex);
                }
                finally
                {
                    Interlocked.Exchange(ref _isRemovingExceedMaxCacheQueueIndex, 0);
                }
            }
        }
    }
}
