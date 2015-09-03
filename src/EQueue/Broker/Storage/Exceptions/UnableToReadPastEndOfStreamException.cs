using System;

namespace EQueue.Broker.Storage
{
    internal class UnableToReadPastEndOfStreamException : Exception
    {
        public UnableToReadPastEndOfStreamException() { }
        public UnableToReadPastEndOfStreamException(string message) : base(message) { }
        public UnableToReadPastEndOfStreamException(string message, Exception innerException) : base(message, innerException) { }
    }
}
