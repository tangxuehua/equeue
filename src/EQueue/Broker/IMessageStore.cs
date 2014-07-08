using System;
using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public interface IMessageStore
    {
        void Recover(Action<long, string, int, long> messageRecoveredCallback);
        void Start();
        void Shutdown();
        QueueMessage StoreMessage(int queueId, long queueOffset, Message message);
        QueueMessage GetMessage(long offset);
        void UpdateMaxAllowToDeleteQueueOffset(string topic, int queueId, long queueOffset);
    }
}
