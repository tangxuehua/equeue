using System;

namespace EQueue.Broker.Storage
{
    public class ChunkNotExistException : Exception
    {
        public ChunkNotExistException(string message) : base(message) { }
    }
}
