using System.IO;

namespace EQueue.Broker
{
    public interface ILogRecord
    {
        void WriteTo(BinaryWriter writer);
        void ReadFrom(BinaryReader reader);
    }
}
