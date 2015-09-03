using System.IO;

namespace EQueue.Broker
{
    public interface ILogRecordParser
    {
        ILogRecord ParseFrom(BinaryReader reader);
    }
}
