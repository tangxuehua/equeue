using System;

namespace EQueue.Broker.Storage
{
    public class ChunkWriteException : Exception
    {
        public ChunkWriteException(string chunkName) : base(chunkName + " write failed.") { }
    }
}
