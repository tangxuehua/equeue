using System.Collections.Generic;
using EQueue.Broker.Storage;

namespace EQueue.Broker.DeleteMessageStrategies
{
    public interface IDeleteMessageStrategy
    {
        IEnumerable<TFChunk> GetAllowDeleteChunks(TFChunkManager chunkManager, long maxMessagePosition);
    }
}
