using System;

namespace EQueue.Broker.Storage
{
    public class BadChunkInDatabaseException : Exception
    {
        public BadChunkInDatabaseException(string message) : base(message)
        {
        }
    }
}
