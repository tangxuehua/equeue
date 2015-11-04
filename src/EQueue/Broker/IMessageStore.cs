using EQueue.Broker.Storage;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public interface IMessageStore
    {
        long MinMessagePosition { get; }
        long CurrentMessagePosition { get; }
        int ChunkCount { get; }
        int MinChunkNum { get; }
        int MaxChunkNum { get; }

        void Load();
        void Start();
        void Shutdown();
        MessageLogRecord StoreMessage(int queueId, long queueOffset, Message message);
        byte[] GetMessageBuffer(long position);
        QueueMessage GetMessage(long position);
        bool IsMessagePositionExist(long position);
        void UpdateMinConsumedMessagePosition(long minConsumedMessagePosition);
    }
}
