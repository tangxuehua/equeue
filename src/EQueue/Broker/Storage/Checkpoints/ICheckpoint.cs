using System;

namespace EQueue.Broker.Storage
{
    public interface ICheckpoint : IDisposable
    {
        string Name { get; }
        void Write(long checkpoint);
        void Flush();
        void Close();

        long Read();
        long ReadNonFlushed();

        bool WaitForFlush(TimeSpan timeout);
    }
}
