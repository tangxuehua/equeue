using System.IO;

namespace EQueue.Broker.Storage
{
    public class QueueLogRecord : LogRecord, ILogRecord
    {
        public long MessageLogPosition { get; private set; }

        public QueueLogRecord()
            : base((byte)LogRecordType.Queue)
        { }
        public QueueLogRecord(long messageLogPosition)
            : base((byte)LogRecordType.Queue)
        {
            MessageLogPosition = messageLogPosition;
        }
        public override void WriteTo(BinaryWriter writer)
        {
            writer.Write(MessageLogPosition);
        }
        public override void ParseFrom(BinaryReader reader)
        {
            MessageLogPosition = reader.ReadInt64();
        }
    }
}
