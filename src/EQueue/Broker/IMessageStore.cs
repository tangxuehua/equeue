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
        MessageLogRecord StoreMessage(int queueId, long queueOffset, Message message, string routingKey);
        byte[] GetMessage(long position);
        void UpdateMinConsumedMessagePosition(long minConsumedMessagePosition);
    }
}
