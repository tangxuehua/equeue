using System;
using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public interface IMessageStore
    {
        IEnumerable<QueueMessage> Messages { get; }
        void Recover();
        void Start();
        void Shutdown();
        QueueMessage StoreMessage(int queueId, long queueOffset, Message message);
        QueueMessage GetMessage(long offset);
        void DeleteMessages(string topic, int queueId, IEnumerable<QueueItem> removedQueueItems, long maxQueueOffset);
    }
}
