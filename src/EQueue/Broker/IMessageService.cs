using System.Collections.Generic;
using EQueue.Broker.Storage;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public interface IMessageService
    {
        void Start();
        void Shutdown();
        MessageStoreResult StoreMessage(Message message, int queueId, string routingKey);
        IEnumerable<MessageLogRecord> GetMessages(string topic, int queueId, long queueOffset, int batchSize);
    }
}
