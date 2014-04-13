using System;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public interface IMessageStore
    {
        long StoreMessage(MessageInfo messageInfo);
        QueueMessage GetMessage(long offset);
        bool RemoveMessage(long offset);
        void Start();
        void Shutdown();
    }
}
