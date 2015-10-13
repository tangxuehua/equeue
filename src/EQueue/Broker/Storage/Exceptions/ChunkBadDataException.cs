using System;

namespace EQueue.Broker.Storage
{
    public class ChunkBadDataException : Exception
    {
        public ChunkBadDataException(string message) : base(message)
        {
        }
    }
}
