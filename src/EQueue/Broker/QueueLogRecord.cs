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
        public void WriteTo(BinaryWriter writer)
        {
            writer.Write(MessageLogPosition);
        }
        public void ReadFrom(BinaryReader reader)
        {
            MessageLogPosition = reader.ReadInt64();
        }
    }
}
