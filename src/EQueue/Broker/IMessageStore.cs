using System;
using System.Collections.Generic;
using EQueue.Broker.Storage;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public interface IMessageStore
    {
        long CurrentMessagePosition { get; }
        bool SupportBatchLoadQueueIndex { get; }
        void Start();
        void Shutdown();
        void DeleteQueueMessage(string topic, int queueId);
        long StoreMessage(int queueId, long queueOffset, Message message, string routingKey);
        MessageLogRecord GetMessage(long logPosition);
        QueueMessage FindMessage(long? offset, string messageId);
        void UpdateConsumedQueueOffset(string topic, int queueId, long queueOffset);
        IDictionary<long, long> BatchLoadQueueIndex(string topic, int queueId, long startQueueOffset);
        IEnumerable<QueueMessage> QueryMessages(string topic, int? queueId, int? code, string routingKey, int pageIndex, int pageSize, out int total);
    }
}
