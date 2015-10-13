using System;

namespace EQueue.Broker.Storage
{
    public class ChunkReadException : Exception
    {
        public ChunkReadException(string message) : base(message) { }
    }
}
