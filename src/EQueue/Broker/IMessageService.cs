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
        long GetQueueCurrentOffset(string topic, int queueId);
        IEnumerable<string> GetAllTopics();
        IEnumerable<int> GetQueueIdsForProducer(string topic);
        IEnumerable<int> GetQueueIdsForConsumer(string topic);
        IList<Queue> QueryQueues(string topic);
        void AddQueue(string topic);
        void RemoveQueue(string topic, int queueId);
        void EnableQueue(string topic, int queueId);
        void DisableQueue(string topic, int queueId);
    }
}
