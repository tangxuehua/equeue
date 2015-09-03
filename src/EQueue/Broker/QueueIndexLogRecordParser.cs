using System.IO;

namespace EQueue.Broker.Storage
{
    public class QueueIndexLogRecordParser : ILogRecordParser
    {
        public ILogRecord ParseFrom(BinaryReader reader)
        {
            var record = new QueueIndexLogRecord();
            record.ParseFrom(reader);
            return record;
        }
    }
}
