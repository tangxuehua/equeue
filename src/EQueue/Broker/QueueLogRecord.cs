using System;
using System.IO;
using EQueue.Broker.Storage.LogRecords;

namespace EQueue.Broker.Storage
{
    public class QueueLogRecord : ILogRecord
    {
        public long MessageLogPosition { get; private set; }

        public QueueLogRecord() { }
        public QueueLogRecord(long messageLogPosition)
        {
            MessageLogPosition = messageLogPosition;
        }
        public void WriteTo(long logPosition, BinaryWriter writer)
        {
            writer.Write(MessageLogPosition);
        }
        public void ReadFrom(byte[] recordBuffer)
        {
            MessageLogPosition = BitConverter.ToInt64(recordBuffer, 0);
        }
    }
}
