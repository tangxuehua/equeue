using System;
using System.Collections.Concurrent;
using System.Threading;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class InMemoryMessageStore : IMessageStore
    {
        private ConcurrentDictionary<long, QueueMessage> _messageDict = new ConcurrentDictionary<long, QueueMessage>();
        private long _currentOffset = -1;

        public long StoreMessage(MessageInfo messageInfo)
        {
            var offset = GetNextOffset();
            var queueMessage = new QueueMessage(
                messageInfo.Message.Topic,
                messageInfo.Message.Body,
                offset,
                messageInfo.Queue.QueueId,
                messageInfo.QueueOffset, DateTime.Now);
            _messageDict[offset] = queueMessage;
            messageInfo.Queue.SetQueueMessage(queueMessage.QueueOffset, queueMessage);
            return offset;
        }

        public void Start() { }
        public void Shutdown() { }
        public QueueMessage GetMessage(long offset)
        {
            QueueMessage queueMessage;
            if (_messageDict.TryGetValue(offset, out queueMessage))
            {
                return queueMessage;
            }
            return null;
        }
        public bool RemoveMessage(long offset)
        {
            QueueMessage queueMessage;
            return _messageDict.TryRemove(offset, out queueMessage);
        }

        private long GetNextOffset()
        {
            return Interlocked.Increment(ref _currentOffset);
        }
    }
}
