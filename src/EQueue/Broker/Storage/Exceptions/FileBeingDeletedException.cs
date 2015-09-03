using System;

namespace EQueue.Broker.Storage
{
    public class FileBeingDeletedException : Exception
    {
        public FileBeingDeletedException() { }
        public FileBeingDeletedException(string message) : base(message) { }
        public FileBeingDeletedException(string message, Exception innerException) : base(message, innerException) { }
    }
}
