using System;

namespace EQueue.Broker.Storage
{
    public class ChunkFileNotExistException : Exception
    {
        public ChunkFileNotExistException(string fileName) : base(string.Format("Chunk file '{0}' not exist.", fileName)) { }
    }
}
