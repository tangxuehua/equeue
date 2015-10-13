using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Utilities;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class InMemoryMessageStore : IMessageStore
    {
        private readonly ConcurrentDictionary<long, QueueMessage> _messageDict = new ConcurrentDictionary<long, QueueMessage>();
        private readonly ConcurrentDictionary<string, long> _queueConsumedOffsetDict = new ConcurrentDictionary<string, long>();
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;
        private long _currentOffset = -1;

        public long CurrentMessageOffset
        {
            get { return _currentOffset; }
        }
        public long PersistedMessageOffset
        {
            get { return _currentOffset; }
        }
        public bool SupportBatchLoadQueueIndex
        {
            get { return false; }
        }

        public InMemoryMessageStore()
        {
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void Recover(IEnumerable<QueueConsumedOffset> queueConsumedOffsets, Action<long, string, int, long> messageRecoveredCallback) { }
        public void Start()
        {
            _scheduleService.StartTask("InMemoryMessageStore.RemoveConsumedMessagesFromMemory", RemoveConsumedMessagesFromMemory, 1000 * 5, 1000 * 5);
        }
        public void Shutdown()
        {
            _scheduleService.StopTask("InMemoryMessageStore.RemoveConsumedMessagesFromMemory");
        }
        public long GetNextMessageOffset()
        {
            return _currentOffset + 1;
        }
        public void IncrementMessageOffset()
        {
            Interlocked.Increment(ref _currentOffset);
        }
        public QueueMessage StoreMessage(int queueId, long messageOffset, long queueOffset, Message message, string routingKey)
        {
            var queueMessage = new QueueMessage(
                ObjectId.GenerateNewStringId(),
                message.Topic,
                message.Code,
                message.Key,
                message.Body,
                messageOffset,
                queueId,
                queueOffset,
                message.CreatedTime,
                DateTime.Now,
                DateTime.Now,
                routingKey);
            _messageDict.TryAdd(messageOffset, queueMessage);
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
        public QueueMessage FindMessage(long? offset, string messageId)
        {
            var predicate = new Func<QueueMessage, bool>(x =>
            {
                var pass = true;
                if (pass && offset != null)
                {
                    pass = x.MessageOffset == offset.Value;
                }
                if (!string.IsNullOrWhiteSpace(messageId))
                {
                    pass = x.MessageId == messageId;
                }
                return pass;
            });
            return _messageDict.Values.SingleOrDefault(predicate);
        }
        public void DeleteQueueMessage(string topic, int queueId)
        {
            var key = string.Format("{0}-{1}", topic, queueId);
            var messages = _messageDict.Values.Where(x => x.Topic == topic && x.QueueId == queueId);
            messages.ForEach(x => _messageDict.Remove(x.MessageOffset));
            _queueConsumedOffsetDict.Remove(key);
        }
        public void UpdateConsumedQueueOffset(string topic, int queueId, long queueOffset)
        {
            var key = string.Format("{0}-{1}", topic, queueId);
            _queueConsumedOffsetDict.AddOrUpdate(key, queueOffset, (currentKey, oldOffset) => queueOffset > oldOffset ? queueOffset : oldOffset);
        }
        public IDictionary<long, long> BatchLoadQueueIndex(string topic, int queueId, long startQueueOffset)
        {
            throw new NotImplementedException();
        }
        public IEnumerable<QueueMessage> QueryMessages(string topic, int? queueId, int? code, string routingKey, int pageIndex, int pageSize, out int total)
        {
            var source = _messageDict.Values;
            var predicate = new Func<QueueMessage, bool>(x =>
            {
                var pass = true;
                if (!string.IsNullOrEmpty(topic))
                {
                    pass = x.Topic == topic;
                }
                if (pass && queueId != null)
                {
                    pass = x.QueueId == queueId.Value;
                }
                if (pass && code != null)
                {
                    pass = x.Code == code.Value;
                }
                if (pass && !string.IsNullOrEmpty(routingKey))
                {
                    pass = x.RoutingKey == routingKey;
                }
                return pass;
            });

            total = source.Count(predicate);
            return source.Where(predicate).Skip((pageIndex - 1) * pageSize).Take(pageSize);
        }

        private void RemoveConsumedMessagesFromMemory()
        {
            var queueMessages = _messageDict.Values;
            foreach (var queueMessage in queueMessages)
            {
                var key = string.Format("{0}-{1}", queueMessage.Topic, queueMessage.QueueId);
                long maxAllowToDeleteQueueOffset;
                if (_queueConsumedOffsetDict.TryGetValue(key, out maxAllowToDeleteQueueOffset) && queueMessage.QueueOffset <= maxAllowToDeleteQueueOffset)
                {
                    _messageDict.Remove(queueMessage.MessageOffset);
                }
            }
        }
    }
}
