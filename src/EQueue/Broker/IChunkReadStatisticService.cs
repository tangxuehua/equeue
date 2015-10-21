using System.Collections.Generic;
using EQueue.Broker.Storage;

namespace EQueue.Broker
{
    public interface IChunkReadStatisticService
    {
        void AddFileReadCount(int chunkNum);
        void AddUnmanagedReadCount(int chunkNum);
        void AddCachedReadCount(int chunkNum);
        void Start();
        void Shutdown();
    }
}
