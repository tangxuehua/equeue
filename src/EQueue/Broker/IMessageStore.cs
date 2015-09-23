using EQueue.Broker.Storage;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public interface IMessageStore
    {
        long MinConsumedMessagePosition { get; }
        long MinMessagePosition { get; }
        long CurrentMessagePosition { get; }
        void Start();
        void Shutdown();
        long StoreMessage(int queueId, long queueOffset, Message message, string routingKey);
        MessageLogRecord GetMessage(long position);
        void UpdateMinConsumedMessagePosition(long minConsumedMessagePosition);
    }
}
