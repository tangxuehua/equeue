using System;
using System.Collections.Generic;
using ECommon.Storage;

namespace EQueue.Broker.DeleteMessageStrategies
{
    public interface IDeleteMessageStrategy
    {
        IEnumerable<Chunk> GetAllowDeleteChunks(ChunkManager chunkManager, Func<long> getMinConsumedMessagePositionFunc);
    }
}
