using EQueue.Broker.Storage;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public interface IMessageStore
    {
        long MinMessagePosition { get; }
        long CurrentMessagePosition { get; }
        void Start();
        void Shutdown();
        void DeleteQueueMessage(string topic, int queueId);
        long StoreMessage(int queueId, long queueOffset, Message message, string routingKey);
        MessageLogRecord GetMessage(long logPosition);
        QueueMessage FindMessage(long? offset, string messageId);
        void UpdateConsumedQueueOffset(string topic, int queueId, long queueOffset);
    }
}
