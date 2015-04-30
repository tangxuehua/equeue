using System;
using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public interface IMessageStore
    {
        long CurrentMessageOffset { get; }
        long PersistedMessageOffset { get; }
        bool SupportBatchLoadQueueIndex { get; }
        void Recover(IEnumerable<QueueConsumedOffset> queueConsumedOffsets, Action<long, string, int, long> messageRecoveredCallback);
        void Start();
        void Shutdown();
        void DeleteQueueMessage(string topic, int queueId);
        long GetNextMessageOffset();
        QueueMessage StoreMessage(int queueId, long messageOffset, long queueOffset, Message message, string routingKey);
        QueueMessage GetMessage(long offset);
        QueueMessage FindMessage(long? offset, string messageId);
        void UpdateConsumedQueueOffset(string topic, int queueId, long queueOffset);
        IDictionary<long, long> BatchLoadQueueIndex(string topic, int queueId, long startQueueOffset);
        IEnumerable<QueueMessage> QueryMessages(string topic, int? queueId, int? code, string routingKey, int pageIndex, int pageSize, out int total);
    }
}
