using System.IO;

namespace EQueue.Broker.Storage
{
    public class MessageLogRecordParser : ILogRecordParser
    {
        public ILogRecord ParseFrom(BinaryReader reader)
        {
            var record = new MessageLogRecord();
            record.ParseFrom(reader);
            return record;
        }
    }
}
