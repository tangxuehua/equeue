using System;

namespace EQueue.Broker.Storage
{
    public class ChunkCreateException : Exception
    {
        public ChunkCreateException(string message) : base(message) { }
    }
}
