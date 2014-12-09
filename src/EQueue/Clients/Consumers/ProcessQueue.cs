using System.Collections.Concurrent;
using System.Collections.Generic;
using ECommon.Extensions;
using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class ProcessQueue
    {
        private ConcurrentDictionary<long, QueueMessage> _messageDict = new ConcurrentDictionary<long, QueueMessage>();
        private long _queueMaxOffset = -1L;

        public bool IsDropped { get; set; }
        public long PreviousConsumedMinQueueOffset { get; private set; }

        public ProcessQueue()
        {
            PreviousConsumedMinQueueOffset = -1L;
        }

        public bool TryUpdatePreviousConsumedMinQueueOffset(long offset)
        {
            if (offset > PreviousConsumedMinQueueOffset)
            {
                PreviousConsumedMinQueueOffset = offset;
                return true;
            }
            return false;
        }
        public void AddMessages(IEnumerable<QueueMessage> messages)
        {
            foreach (var message in messages)
            {
                if (_messageDict.TryAdd(message.QueueOffset, message))
                {
                    if (message.QueueOffset > _queueMaxOffset)
                    {
                        _queueMaxOffset = message.QueueOffset;
                    }
                }
            }
        }
        public void RemoveMessage(QueueMessage message)
        {
            _messageDict.Remove(message.QueueOffset);
        }
        public int GetMessageCount()
        {
            return _messageDict.Count;
        }
        public long GetConsumedMinQueueOffset()
        {
            var currentMinQueueOffset = _queueMaxOffset + 1;
            foreach (var offset in _messageDict.Keys)
            {
                if (offset < currentMinQueueOffset)
                {
                    currentMinQueueOffset = offset;
                }
            }
            return currentMinQueueOffset - 1;
        }
    }
}
