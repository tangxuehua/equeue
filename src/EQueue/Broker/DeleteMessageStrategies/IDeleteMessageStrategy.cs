using System.Collections.Generic;
using EQueue.Broker.Storage;

namespace EQueue.Broker.DeleteMessageStrategies
{
    public interface IDeleteMessageStrategy
    {
        IEnumerable<Chunk> GetAllowDeleteChunks(ChunkManager chunkManager, long maxMessagePosition);
    }
}
