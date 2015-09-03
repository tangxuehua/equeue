using System;

namespace EQueue.Broker.Storage
{
    public class CorruptDatabaseException : Exception
    {
        public CorruptDatabaseException(Exception inner) : base("Corrupt database detected.", inner) { }
    }
}
