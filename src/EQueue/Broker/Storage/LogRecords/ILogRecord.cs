using System.IO;

namespace EQueue.Broker
{
    public interface ILogRecord
    {
        byte Type { get; }
        void WriteTo(BinaryWriter writer);
        void ParseFrom(BinaryReader reader);
    }
}
