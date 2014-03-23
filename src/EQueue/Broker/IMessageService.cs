using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public interface IMessageService
    {
        void Start();
        void Shutdown();
        void SetBrokerContrller(BrokerController brokerController);
        MessageStoreResult StoreMessage(Message message, int queueId);
        IEnumerable<QueueMessage> GetMessages(string topic, int queueId, long queueOffset, int batchSize);
        long GetQueueOffset(string topic, int queueId);
        int GetTopicQueueCount(string topic);
    }
}
