using System;
using System.Collections.Generic;
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
        void StoreMessageAsync(IQueue queue, Message message, Action<MessageLogRecord, object> callback, object parameter, string producerAddress);
        void BatchStoreMessageAsync(IQueue queue, IEnumerable<Message> messages, Action<BatchMessageLogRecord, object> callback, object parameter, string producerAddress);
        byte[] GetMessageBuffer(long position);
        QueueMessage GetMessage(long position);
        bool IsMessagePositionExist(long position);
        Func<long> GetMinConsumedMessagePositionFunc { get; set; }
    }
}
