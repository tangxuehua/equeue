using System.IO;

namespace EQueue.Broker
{
    public interface ILogRecord
    {
        byte Type { get; }
        byte Version { get; }
        long LogPosition { get; set; }
        void WriteTo(BinaryWriter writer);
        void ParseFrom(BinaryReader reader);
    }
}
