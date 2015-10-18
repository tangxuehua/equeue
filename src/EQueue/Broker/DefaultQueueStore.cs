using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Utilities;

namespace EQueue.Broker
{
    public class DefaultQueueStore : IQueueStore
    {
        private readonly ConcurrentDictionary<string, Queue> _queueDict;
        private readonly IMessageStore _messageStore;
        private readonly IConsumeOffsetStore _consumeOffsetStore;
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;
        private int _isScanningMinConsumedMessagePosition;
        private int _isDeletingQueueMessage;

        public DefaultQueueStore(IMessageStore messageStore, IConsumeOffsetStore consumeOffsetStore, IScheduleService scheduleService, ILoggerFactory loggerFactory)
        {
            _queueDict = new ConcurrentDictionary<string, Queue>();
            _messageStore = messageStore;
            _consumeOffsetStore = consumeOffsetStore;
            _scheduleService = scheduleService;
            _logger = loggerFactory.Create(GetType().FullName);
        }

        public void Clean()
        {
            CleanQueueChunks();
        }
        public void Start()
        {
            LoadQueues();
            _scheduleService.StartTask(string.Format("{0}.ScanMinConsumedMessagePosition", this.GetType().Name), ScanMinConsumedMessagePosition, 1000 * 5, 1000 * 5);
            _scheduleService.StartTask(string.Format("{0}.DeleteQueueMessages", this.GetType().Name), DeleteQueueMessages, 5 * 1000, BrokerController.Instance.Setting.DeleteQueueMessagesInterval);
        }
        public void Shutdown()
        {
            CloseQueues();
            _scheduleService.StopTask(string.Format("{0}.DeleteMessages", this.GetType().Name));
        }
        public IEnumerable<string> GetAllTopics()
        {
            return _queueDict.Values.Select(x => x.Topic).Distinct();
        }
        public int GetAllQueueCount()
        {
            return _queueDict.Count;
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
                return queue.NextOffset - 1;
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
        public long GetAllQueueUnConusmedMessageCount()
        {
            var minConsumedQueueOffset = 0L;

            foreach (var queue in _queueDict.Values)
            {
                var offset = _consumeOffsetStore.GetMinConsumedOffset(queue.Topic, queue.QueueId);
                if (minConsumedQueueOffset == 0L && offset > 0)
                {
                    minConsumedQueueOffset = offset;
                }
                else if (offset < minConsumedQueueOffset)
                {
                    minConsumedQueueOffset = offset;
                }
            }

            return minConsumedQueueOffset;
        }
        public void CreateTopic(string topic, int initialQueueCount)
        {
            lock (this)
            {
                Ensure.NotNullOrEmpty(topic, "topic");
                Ensure.Positive(initialQueueCount, "initialQueueCount");
                if (initialQueueCount > BrokerController.Instance.Setting.TopicMaxQueueCount)
                {
                    throw new ArgumentException(string.Format("Initial queue count {0} cannot bigger than max queue count {1}.", initialQueueCount, BrokerController.Instance.Setting.TopicMaxQueueCount));
                }
                for (var index = 0; index < initialQueueCount; index++)
                {
                    LoadQueue(topic, index);
                }
            };
        }
        public void AddQueue(string topic)
        {
            lock (this)
            {
                Ensure.NotNullOrEmpty(topic, "topic");
                var queues = _queueDict.Values.Where(x => x.Topic == topic);
                if (queues.Count() >= BrokerController.Instance.Setting.TopicMaxQueueCount)
                {
                    throw new ArgumentException(string.Format("Queue count cannot bigger than {0}.", BrokerController.Instance.Setting.TopicMaxQueueCount));
                }
                var queueId = queues.Count() == 0 ? 0 : queues.Max(x => x.QueueId) + 1;
                LoadQueue(topic, queueId);
            }
        }
        public void RemoveQueue(string topic, int queueId)
        {
            //TODO
            //lock (this)
            //{
            //    var key = CreateQueueKey(topic, queueId);
            //    Queue queue;
            //    if (!_queueDict.TryGetValue(key, out queue))
            //    {
            //        return;
            //    }

            //    //检查队列状态是否是已禁用
            //    if (queue.Setting.Status != QueueStatus.Disabled)
            //    {
            //        throw new Exception("Queue status is not disabled, cannot be deleted.");
            //    }
            //    //检查是否有未消费完的消息
            //    if (queue.GetMessageRealCount() > 0L)
            //    {
            //        throw new Exception("Queue is not allowed to delete as there are messages exist in this queue.");
            //    }

            //    //删除队列消息
            //    _messageStore.DeleteQueueMessage(topic, queueId);

            //    //删除队列消费进度信息
            //    _offsetStore.DeleteQueueOffset(topic, queueId);

            //    //删除队列
            //    _queueStore.DeleteQueue(queue);

            //    //从内存移除队列
            //    _queueDict.Remove(key);
            //}
        }
        public void EnableQueue(string topic, int queueId)
        {
            lock (this)
            {
                var queue = GetQueue(topic, queueId);
                if (queue != null)
                {
                    queue.Enable();
                }
            }
        }
        public void DisableQueue(string topic, int queueId)
        {
            lock (this)
            {
                var queue = GetQueue(topic, queueId);
                if (queue != null)
                {
                    queue.Disable();
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
        public IEnumerable<Queue> QueryQueues(string topic)
        {
            return _queueDict.Values.Where(x => x.Topic.Contains(topic));
        }
        public IEnumerable<Queue> GetOrCreateQueues(string topic, QueueStatus? status = null)
        {
            lock (this)
            {
                var queues = _queueDict.Values.Where(x => x.Topic == topic);
                if (queues.IsEmpty() && BrokerController.Instance.Setting.AutoCreateTopic)
                {
                    CreateTopic(topic, BrokerController.Instance.Setting.TopicDefaultQueueCount);
                    queues = _queueDict.Values.Where(x => x.Topic == topic);
                }
                if (status != null)
                {
                    return queues.Where(x => x.Setting.Status == status.Value);
                }
                return queues;
            }
        }
        public IEnumerable<Queue> FindQueues(string topic, QueueStatus? status = null)
        {
            var queues = _queueDict.Values.Where(x => x.Topic == topic);
            if (status != null)
            {
                return queues.Where(x => x.Setting.Status == status.Value);
            }
            return queues;
        }

        private void CleanQueueChunks()
        {
            var chunkConfig = BrokerController.Instance.Setting.QueueChunkConfig;
            if (!Directory.Exists(chunkConfig.BasePath))
            {
                return;
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
                    new Queue(topic, queueId).CleanChunks();
                }
            }
        }
        private void LoadQueues()
        {
            _queueDict.Clear();

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
            var key = CreateQueueKey(topic, queueId);
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
        private static string CreateQueueKey(string topic, int queueId)
        {
            return string.Format("{0}-{1}", topic, queueId);
        }
        private void ScanMinConsumedMessagePosition()
        {
            if (Interlocked.CompareExchange(ref _isScanningMinConsumedMessagePosition, 1, 0) == 0)
            {
                try
                {
                    foreach (var queue in _queueDict.Values)
                    {
                        var minConsumedQueueOffset = _consumeOffsetStore.GetMinConsumedOffset(queue.Topic, queue.QueueId);
                        if (minConsumedQueueOffset >= 0)
                        {
                            var currentOffset = queue.NextOffset - 1;
                            if (minConsumedQueueOffset > currentOffset)
                            {
                                minConsumedQueueOffset = currentOffset;
                            }
                            var messagePosition = queue.GetMessagePosition(minConsumedQueueOffset);
                            if (messagePosition >= 0)
                            {
                                _messageStore.UpdateMinConsumedMessagePosition(messagePosition);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.Error("Scan min consumed message position has exception.", ex);
                }
                finally
                {
                    Interlocked.Exchange(ref _isScanningMinConsumedMessagePosition, 0);
                }
            }
        }
        private void DeleteQueueMessages()
        {
            if (Interlocked.CompareExchange(ref _isDeletingQueueMessage, 1, 0) == 0)
            {
                try
                {
                    foreach (var queue in _queueDict.Values)
                    {
                        queue.DeleteMessages(_messageStore.MinMessagePosition);
                    }
                }
                catch (Exception ex)
                {
                    _logger.Error("Delete queue messages has exception.", ex);
                }
                finally
                {
                    Interlocked.Exchange(ref _isDeletingQueueMessage, 0);
                }
            }
        }
    }
}
