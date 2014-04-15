using System;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public interface IMessageStore
    {
        void Start();
        void Shutdown();
        QueueMessage StoreMessage(int queueId, long queueOffset, Message message);
        QueueMessage GetMessage(long offset);
        bool RemoveMessage(long offset);
    }
}
