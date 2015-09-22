using System.IO;

namespace EQueue.Broker.Storage
{
    public class QueueLogRecordParser : ILogRecordParser
    {
        public ILogRecord ParseFrom(BinaryReader reader)
        {
            var record = new QueueLogRecord();
            record.ParseFrom(reader);
            return record;
        }
    }
}
