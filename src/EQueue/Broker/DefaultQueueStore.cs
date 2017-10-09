using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Storage.Exceptions;
using ECommon.Utilities;
using EQueue.Protocols.Brokers;

namespace EQueue.Broker
{
    public class DefaultQueueStore : IQueueStore
    {
        private readonly ConcurrentDictionary<QueueKey, Queue> _queueDict;
        private readonly IMessageStore _messageStore;
        private readonly IConsumeOffsetStore _consumeOffsetStore;
        private readonly IScheduleService _scheduleService;
        private readonly ITpsStatisticService _tpsStatisticService;
        private readonly ILogger _logger;
        private readonly object _lockObj = new object();
        private int _isDeletingQueueMessage;

        public DefaultQueueStore(IMessageStore messageStore, IConsumeOffsetStore consumeOffsetStore, IScheduleService scheduleService, ITpsStatisticService tpsStatisticService, ILoggerFactory loggerFactory)
        {
            _queueDict = new ConcurrentDictionary<QueueKey, Queue>();
            _messageStore = messageStore;
            _consumeOffsetStore = consumeOffsetStore;
            _scheduleService = scheduleService;
            _tpsStatisticService = tpsStatisticService;
            _logger = loggerFactory.Create(GetType().FullName);
            _messageStore.GetMinConsumedMessagePositionFunc = new Func<long>(GetMinConusmedMessagePosition);
        }

        public void Load()
        {
            LoadQueues();
        }
        public void Start()
        {
            _scheduleService.StartTask("DeleteQueueMessages", DeleteQueueMessages, 5 * 1000, BrokerController.Instance.Setting.DeleteQueueMessagesInterval);
        }
        public void Shutdown()
        {
            CloseQueues();
            _scheduleService.StopTask("DeleteQueueMessages");
        }
        public IEnumerable<string> GetAllTopics()
        {
            return _queueDict.Values.Select(x => x.Topic).Distinct();
        }
        public IEnumerable<Queue> GetAllQueues()
        {
            return _queueDict.Values.Where(x => !x.Setting.IsDeleted).ToList();
        }
        public IList<TopicQueueInfo> GetTopicQueueInfoList(string topic = null)
        {
            var topicQueueInfoList = new List<TopicQueueInfo>();
            var queueList = default(IList<Queue>);
            if (string.IsNullOrEmpty(topic))
            {
                queueList = GetAllQueues().ToList();
            }
            else
            {
                queueList = GetQueues(topic).ToList();
            }
            var foundQueues = queueList.OrderBy(x => x.Topic).ThenBy(x => x.QueueId);
            foreach (var queue in foundQueues)
            {
                var topicQueueInfo = new TopicQueueInfo();
                topicQueueInfo.Topic = queue.Topic;
                topicQueueInfo.QueueId = queue.QueueId;
                topicQueueInfo.QueueCurrentOffset = queue.NextOffset - 1;
                topicQueueInfo.QueueMinOffset = queue.GetMinQueueOffset();
                topicQueueInfo.QueueMinConsumedOffset = _consumeOffsetStore.GetMinConsumedOffset(queue.Topic, queue.QueueId);
                topicQueueInfo.ProducerVisible = queue.Setting.ProducerVisible;
                topicQueueInfo.ConsumerVisible = queue.Setting.ConsumerVisible;
                topicQueueInfo.QueueNotConsumeCount = topicQueueInfo.CalculateQueueNotConsumeCount();
                topicQueueInfo.SendThroughput = _tpsStatisticService.GetTopicSendThroughput(queue.Topic, queue.QueueId);
                topicQueueInfoList.Add(topicQueueInfo);
            }

            return topicQueueInfoList;
        }
        public int GetAllQueueCount()
        {
            return _queueDict.Count;
        }
        public bool IsTopicExist(string topic)
        {
            return _queueDict.Values.Any(x => x.Topic == topic);
        }
        public bool IsQueueExist(QueueKey queueKey)
        {
            return GetQueue(queueKey) != null;
        }
        public bool IsQueueExist(string topic, int queueId)
        {
            return GetQueue(topic, queueId) != null;
        }
        public long GetQueueCurrentOffset(string topic, int queueId)
        {
            var key = new QueueKey(topic, queueId);
            Queue queue;
            if (_queueDict.TryGetValue(key, out queue))
            {
                return queue.NextOffset - 1;
            }
            return -1;
        }
        public long GetQueueMinOffset(string topic, int queueId)
        {
            var key = new QueueKey(topic, queueId);
            Queue queue;
            if (_queueDict.TryGetValue(key, out queue))
            {
                return queue.GetMinQueueOffset();
            }
            return -1;
        }
        public long GetTotalUnConusmedMessageCount()
        {
            var totalCount = 0L;

            foreach (var currentQueue in _queueDict.Values)
            {
                var minConsumedOffset = _consumeOffsetStore.GetMinConsumedOffset(currentQueue.Topic, currentQueue.QueueId);
                var queueCurrentOffset = currentQueue.NextOffset - 1;
                if (queueCurrentOffset > minConsumedOffset)
                {
                    var count = queueCurrentOffset - minConsumedOffset;
                    totalCount += count;
                }
            }

            return totalCount;
        }
        public IEnumerable<int> CreateTopic(string topic, int? initialQueueCount = null)
        {
            lock (_lockObj)
            {
                Ensure.NotNullOrEmpty(topic, "topic");

                if (initialQueueCount != null)
                {
                    Ensure.Positive(initialQueueCount.Value, "initialQueueCount");
                }
                else
                {
                    initialQueueCount = BrokerController.Instance.Setting.TopicDefaultQueueCount;
                }

                if (IsTopicExist(topic))
                {
                    return _queueDict.Values.Where(x => x.Topic == topic && !x.Setting.IsDeleted).Select(x => x.QueueId).ToList();
                }
                if (initialQueueCount > BrokerController.Instance.Setting.TopicMaxQueueCount)
                {
                    throw new ArgumentException(string.Format("Initial queue count {0} cannot bigger than max queue count {1}.", initialQueueCount, BrokerController.Instance.Setting.TopicMaxQueueCount));
                }

                for (var index = 0; index < initialQueueCount; index++)
                {
                    LoadQueue(topic, index);
                }
                return _queueDict.Values.Where(x => x.Topic == topic && !x.Setting.IsDeleted).Select(x => x.QueueId).ToList();
            }
        }
        public void DeleteTopic(string topic)
        {
            lock (_lockObj)
            {
                Ensure.NotNullOrEmpty(topic, "topic");

                var queues = _queueDict.Values.Where(x => x.Topic == topic).OrderBy(x => x.QueueId);
                foreach (var queue in queues)
                {
                    CheckQueueAllowToDelete(queue);
                }
                foreach (var queue in queues)
                {
                    DeleteQueue(queue.Topic, queue.QueueId);
                }

                if (!BrokerController.Instance.Setting.IsMessageStoreMemoryMode)
                {
                    var topicPath = Path.Combine(BrokerController.Instance.Setting.QueueChunkConfig.BasePath, topic);
                    if (Directory.Exists(topicPath))
                    {
                        Directory.Delete(topicPath);
                    }
                }
            }
        }
        public void AddQueue(string topic)
        {
            lock (_lockObj)
            {
                Ensure.NotNullOrEmpty(topic, "topic");
                var queues = _queueDict.Values.Where(x => x.Topic == topic);
                if (queues.Count() >= BrokerController.Instance.Setting.TopicMaxQueueCount)
                {
                    throw new ArgumentException(string.Format("Queue count cannot bigger than {0}.", BrokerController.Instance.Setting.TopicMaxQueueCount));
                }
                var queueId = queues.Count() == 0 ? 0 : queues.Max(x => x.QueueId) + 1;
                if (!IsQueueExist(topic, queueId))
                {
                    LoadQueue(topic, queueId);
                }
            }
        }
        public void SetProducerVisible(string topic, int queueId, bool visible)
        {
            lock (_lockObj)
            {
                var queue = GetQueue(topic, queueId);
                if (queue != null)
                {
                    queue.SetProducerVisible(visible);
                }
            }
        }
        public void SetConsumerVisible(string topic, int queueId, bool visible)
        {
            lock (_lockObj)
            {
                var queue = GetQueue(topic, queueId);
                if (queue != null)
                {
                    queue.SetConsumerVisible(visible);
                }
            }
        }
        public void DeleteQueue(string topic, int queueId)
        {
            lock (_lockObj)
            {
                var key = new QueueKey(topic, queueId);
                Queue queue;
                if (!_queueDict.TryGetValue(key, out queue))
                {
                    return;
                }

                //检查队列是否可删除
                CheckQueueAllowToDelete(queue);

                //删除队列的消费进度信息
                _consumeOffsetStore.DeleteConsumeOffset(queue.Key);

                //删除队列本身，包括所有的文件
                queue.Delete();

                //最后将队列从字典中移除
                _queueDict.Remove(key);

                //如果当前Broker上一个队列都没有了，则清空整个Broker下的所有文件
                if (_queueDict.IsEmpty)
                {
                    BrokerController.Instance.Clean();
                }
            }
        }
        public Queue GetQueue(string topic, int queueId)
        {
            return GetQueue(new QueueKey(topic, queueId));
        }
        public IEnumerable<Queue> GetQueues(string topic, bool autoCreate = false)
        {
            lock (_lockObj)
            {
                var queues = _queueDict.Values.Where(x => x.Topic == topic && !x.Setting.IsDeleted);
                if (queues.IsEmpty() && autoCreate)
                {
                    CreateTopic(topic, BrokerController.Instance.Setting.TopicDefaultQueueCount);
                    queues = _queueDict.Values.Where(x => x.Topic == topic && !x.Setting.IsDeleted);
                }
                return queues;
            }
        }

        private long GetMinConusmedMessagePosition()
        {
            var minConsumedQueueOffset = -1L;
            var queue = default(Queue);
            var hasConsumerQueues = _queueDict.Values.Where(x => BrokerController.Instance.ConsumerManager.IsConsumerExistForQueue(x.Topic, x.QueueId)).ToList();

            foreach (var currentQueue in hasConsumerQueues)
            {
                var offset = _consumeOffsetStore.GetMinConsumedOffset(currentQueue.Topic, currentQueue.QueueId);
                var queueCurrentOffset = currentQueue.NextOffset - 1;
                if (offset > queueCurrentOffset)
                {
                    offset = queueCurrentOffset;
                }

                if (minConsumedQueueOffset == -1L && offset >= 0)
                {
                    minConsumedQueueOffset = offset;
                    queue = currentQueue;
                }
                else if (offset < minConsumedQueueOffset)
                {
                    minConsumedQueueOffset = offset;
                    queue = currentQueue;
                }
            }

            if (queue != null && minConsumedQueueOffset >= 0)
            {
                try
                {
                    return queue.GetMessagePosition(minConsumedQueueOffset, out int tagCode, false);
                }
                catch (ChunkNotExistException)
                {
                    return -1;
                }
            }

            return -1L;
        }
        private void CheckQueueAllowToDelete(Queue queue)
        {
            //检查队列对Producer或Consumer是否可见，如果可见是不允许删除的
            if (queue.Setting.ProducerVisible || queue.Setting.ConsumerVisible)
            {
                throw new Exception(string.Format("Queue[topic:{0},queueId:{1}] is visible to producer or consumer, cannot be delete.", queue.Topic, queue.QueueId));
            }
            //检查是否有未消费完的消息
            var minConsumedOffset = _consumeOffsetStore.GetMinConsumedOffset(queue.Topic, queue.QueueId);
            var queueCurrentOffset = queue.NextOffset - 1;
            if (minConsumedOffset < queueCurrentOffset)
            {
                throw new Exception(string.Format("Queue[topic:{0},queueId:{1}] is not allowed to delete as there are messages haven't been consumed, not consumed messageCount: {2}", queue.Topic, queue.QueueId, queueCurrentOffset - minConsumedOffset));
            }
        }
        private Queue GetQueue(QueueKey key)
        {
            Queue queue;
            if (_queueDict.TryGetValue(key, out queue) && !queue.Setting.IsDeleted)
            {
                return queue;
            }
            return null;
        }
        private void LoadQueues()
        {
            _queueDict.Clear();

            if (BrokerController.Instance.Setting.IsMessageStoreMemoryMode)
            {
                return;
            }

            var chunkConfig = BrokerController.Instance.Setting.QueueChunkConfig;
            if (!Directory.Exists(chunkConfig.BasePath))
            {
                Directory.CreateDirectory(chunkConfig.BasePath);
            }
            var topicPathList = Directory
                            .EnumerateDirectories(chunkConfig.BasePath, "*", SearchOption.TopDirectoryOnly)
                            .OrderBy(x => x, StringComparer.CurrentCultureIgnoreCase)
                            .ToArray();
            foreach (var topicPath in topicPathList)
            {
                var queuePathList = Directory
                            .EnumerateDirectories(topicPath, "*", SearchOption.TopDirectoryOnly)
                            .OrderBy(x => x, StringComparer.CurrentCultureIgnoreCase)
                            .ToArray();
                foreach (var queuePath in queuePathList)
                {
                    var items = queuePath.Split('\\');
                    var queueId = int.Parse(items[items.Length - 1]);
                    var topic = items[items.Length - 2];
                    LoadQueue(topic, queueId);
                }
            }
        }
        private void LoadQueue(string topic, int queueId)
        {
            var queue = new Queue(topic, queueId);
            queue.Load();
            if (queue.Setting.IsDeleted)
            {
                return;
            }
            var key = new QueueKey(topic, queueId);
            _queueDict.TryAdd(key, queue);
        }
        private void CloseQueues()
        {
            foreach (var queue in _queueDict.Values)
            {
                try
                {
                    queue.Close();
                }
                catch (Exception ex)
                {
                    _logger.Error(string.Format("Close queue failed, topic: {0}, queueId: {1}", queue.Topic, queue.QueueId), ex);
                }
            }
            _queueDict.Clear();
        }
        private void DeleteQueueMessages()
        {
            if (Interlocked.CompareExchange(ref _isDeletingQueueMessage, 1, 0) == 0)
            {
                try
                {
                    var queues = _queueDict.OrderBy(x => x.Key).Select(x => x.Value).ToList();
                    foreach (var queue in queues)
                    {
                        try
                        {
                            queue.DeleteMessages(_messageStore.MinMessagePosition);
                        }
                        catch (Exception ex)
                        {
                            _logger.Error(string.Format("Delete queue (topic: {0}, queueId: {1}) messages has exception.", queue.Topic, queue.QueueId), ex);
                        }
                    }
                }
                finally
                {
                    Interlocked.Exchange(ref _isDeletingQueueMessage, 0);
                }
            }
        }
    }
}
