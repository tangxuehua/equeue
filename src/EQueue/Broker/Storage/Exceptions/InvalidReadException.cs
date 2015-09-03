using System;

namespace EQueue.Broker.Storage
{
    public class InvalidReadException : Exception
    {
        public InvalidReadException(string message) : base(message) { }
    }
}
