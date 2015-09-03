using System;

namespace EQueue.Broker.Storage
{
    public class ChunkNotFoundException : Exception
    {
        public ChunkNotFoundException(string chunkName) : base(chunkName + " not found.") { }
    }
}
