using System;
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
        void StoreMessageAsync(IQueue queue, Message message, Action<MessageLogRecord, object> callback, object parameter);
        byte[] GetMessageBuffer(long position);
        QueueMessage GetMessage(long position);
        bool IsMessagePositionExist(long position);
        void UpdateMinConsumedMessagePosition(long minConsumedMessagePosition);
    }
}
