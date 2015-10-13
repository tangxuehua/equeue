using System;

namespace EQueue.Broker.Storage
{
    public class ChunkCompleteException : Exception
    {
        public ChunkCompleteException(string message) : base(message) { }
    }
}
