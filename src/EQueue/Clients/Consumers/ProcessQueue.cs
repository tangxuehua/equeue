using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using EQueue.Common;
using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class ProcessQueue
    {
        private ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();
        private IDictionary<long, Message> _messageDict = new SortedDictionary<long, Message>();
        private long _queueOffsetMax = 0L;

        public void AddMessages(IEnumerable<QueueMessage> messages)
        {
            AtomWrite(_lock, () =>
            {
                foreach (var message in messages)
                {
                    _messageDict[message.QueueOffset] = message;
                    _queueOffsetMax = message.QueueOffset;
                }
            });
        }
        public long RemoveMessages(IEnumerable<QueueMessage> messages)
        {
            var result = -1L;

            AtomWrite(_lock, () =>
            {
                if (_messageDict.Count > 0)
                {
                    result = _queueOffsetMax + 1;
                    foreach (var message in messages)
                    {
                        _messageDict.Remove(message.QueueOffset);
                    }
                    if (_messageDict.Count > 0)
                    {
                        result = _messageDict.Keys.First();
                    }
                }
            });

            return result;
        }
        public int GetMessageCount()
        {
            return _messageDict.Count;
        }
        public long GetMessageSpan()
        {
            return _messageDict.Keys.LastOrDefault() - _messageDict.Keys.FirstOrDefault();
        }

        private static void AtomWrite(ReaderWriterLockSlim readerWriterLockSlim, Action action)
        {
            readerWriterLockSlim.EnterWriteLock();
            try
            {
                action();
            }
            finally
            {
                readerWriterLockSlim.ExitWriteLock();
            }
        }
    }
}
