using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Scheduling;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class InMemoryMessageStore : IMessageStore
    {
        private readonly ConcurrentDictionary<long, QueueMessage> _messageDict = new ConcurrentDictionary<long, QueueMessage>();
        private readonly ConcurrentDictionary<string, long> _queueOffsetDict = new ConcurrentDictionary<string, long>();
        private readonly InMemoryMessageStoreSetting _setting;
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;
        private long _currentOffset = -1;
        private int _removeMessageFromMemoryTaskId;

        public bool SupportBatchLoadQueueIndex
        {
            get { return false; }
        }

        public InMemoryMessageStore(InMemoryMessageStoreSetting setting)
        {
            _setting = setting;
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void Recover(Action<long, string, int, long> messageRecoveredCallback) { }
        public void Start()
        {
            _removeMessageFromMemoryTaskId = _scheduleService.ScheduleTask("InMemoryMessageStore.RemoveConsumedMessagesFromMemory", RemoveConsumedMessagesFromMemory, _setting.RemoveMessageFromMemoryInterval, _setting.RemoveMessageFromMemoryInterval);
        }
        public void Shutdown()
        {
            _scheduleService.ShutdownTask(_removeMessageFromMemoryTaskId);
        }
        public QueueMessage StoreMessage(int queueId, long queueOffset, Message message, string routingKey)
        {
            var nextOffset = GetNextOffset();
            var queueMessage = new QueueMessage(message.Topic, message.Code, message.Body, nextOffset, queueId, queueOffset, DateTime.Now, routingKey);
            _messageDict[nextOffset] = queueMessage;
            return queueMessage;
        }
        public QueueMessage GetMessage(long offset)
        {
            QueueMessage queueMessage;
            if (_messageDict.TryGetValue(offset, out queueMessage))
            {
                return queueMessage;
            }
            return null;
        }
        public void UpdateMaxAllowToDeleteQueueOffset(string topic, int queueId, long queueOffset)
        {
            var key = string.Format("{0}-{1}", topic, queueId);
            _queueOffsetDict.AddOrUpdate(key, queueOffset, (currentKey, oldOffset) => queueOffset > oldOffset ? queueOffset : oldOffset);
        }
        public IDictionary<long, long> BatchLoadQueueIndex(string topic, int queueId, long startQueueOffset)
        {
            throw new NotImplementedException();
        }
        public IEnumerable<QueueMessage> QueryMessages(string topic, int? queueId, int? code)
        {
            return _messageDict.Values.ToList();
        }

        private void RemoveConsumedMessagesFromMemory()
        {
            var queueMessages = _messageDict.Values.ToList();
            foreach (var queueMessage in queueMessages)
            {
                var key = string.Format("{0}-{1}", queueMessage.Topic, queueMessage.QueueId);
                long maxAllowToDeleteQueueOffset;
                if (_queueOffsetDict.TryGetValue(key, out maxAllowToDeleteQueueOffset) && queueMessage.QueueOffset <= maxAllowToDeleteQueueOffset)
                {
                    QueueMessage removedQueueMessage;
                    if (!_messageDict.TryRemove(queueMessage.MessageOffset, out removedQueueMessage))
                    {
                        _logger.ErrorFormat("Failed to remove consumed message, messageOffset:{0}", queueMessage.MessageOffset);
                    }
                }
            }
        }
        private long GetNextOffset()
        {
            return Interlocked.Increment(ref _currentOffset);
        }
    }
}
