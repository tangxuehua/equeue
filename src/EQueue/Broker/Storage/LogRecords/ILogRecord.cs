using System.IO;

namespace EQueue.Broker
{
    public interface ILogRecord
    {
        void WriteTo(long logPosition, BinaryWriter writer);
        void ReadFrom(int length, BinaryReader reader);
    }
}
