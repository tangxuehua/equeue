using System;
using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public interface IMessageStore
    {
        bool SupportBatchLoadQueueIndex { get; }
        void Recover(Action<long, string, int, long> messageRecoveredCallback);
        void Start();
        void Shutdown();
        QueueMessage StoreMessage(int queueId, long queueOffset, Message message, string routingKey);
        QueueMessage GetMessage(long offset);
        void UpdateMaxAllowToDeleteQueueOffset(string topic, int queueId, long queueOffset);
        IDictionary<long, long> BatchLoadQueueIndex(string topic, int queueId, long startQueueOffset);
        IEnumerable<QueueMessage> QueryMessages(string topic, int? queueId, int? code);
    }
}
