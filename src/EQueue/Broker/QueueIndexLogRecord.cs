using System.IO;

namespace EQueue.Broker.Storage
{
    public class QueueIndexLogRecord : LogRecord, ILogRecord
    {
        public long QueueOffset { get; private set; }
        public long MessageLogPosition { get; private set; }

        public QueueIndexLogRecord()
            : base((byte)LogRecordType.QueueIndex)
        { }
        public QueueIndexLogRecord(byte version, long logPosition, long queueOffset, long messageLogPosition)
            : base((byte)LogRecordType.QueueIndex)
        {
            QueueOffset = queueOffset;
            MessageLogPosition = messageLogPosition;
        }
        public override void WriteTo(BinaryWriter writer)
        {
            base.WriteTo(writer);

            writer.Write(QueueOffset);
            writer.Write(MessageLogPosition);
        }
        public override void ParseFrom(BinaryReader reader)
        {
            base.ParseFrom(reader);

            QueueOffset = reader.ReadInt64();
            MessageLogPosition = reader.ReadInt64();
        }
    }
}
