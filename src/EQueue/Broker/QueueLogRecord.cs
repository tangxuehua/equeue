using System.IO;

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
        public void ReadFrom(int length, BinaryReader reader)
        {
            MessageLogPosition = reader.ReadInt64();
        }
    }
}
