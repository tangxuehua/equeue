using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public interface IMessageService
    {
        void Start();
        void Shutdown();
        void SetBrokerContrller(BrokerController brokerController);
        BrokerStatisticInfo GetBrokerStatisticInfo();
        MessageStoreResult StoreMessage(Message message, int queueId, string routingKey);
        IEnumerable<QueueMessage> GetMessages(string topic, int queueId, long queueOffset, int batchSize);
        bool IsQueueExist(string topic, int queueId);
        long GetQueueCurrentOffset(string topic, int queueId);
        IEnumerable<string> GetAllTopics();
        IEnumerable<int> GetQueueIdsForProducer(string topic);
        IEnumerable<int> GetQueueIdsForConsumer(string topic);
        long GetQueueMinOffset(string topic, int queueId);
        IList<Queue> QueryQueues(string topic);
        void AddQueue(string topic);
        void RemoveQueue(string topic, int queueId);
        void EnableQueue(string topic, int queueId);
        void DisableQueue(string topic, int queueId);
        IEnumerable<QueueMessage> QueryMessages(string topic, int? queueId, int? code, string routingKey, int pageIndex, int pageSize, out int total);
        QueueMessage GetMessageDetail(long? messageOffset, string messageId);
    }
}
