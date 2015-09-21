using System.IO;
using ECommon.Utilities;

namespace EQueue.Broker.Storage
{
    public abstract class LogRecord
    {
        public byte Type { get; private set; }

        protected LogRecord(byte type)
        {
            Ensure.Positive(type, "type");

            Type = type;
        }

        public abstract void WriteTo(BinaryWriter writer);
        public abstract void ParseFrom(BinaryReader reader);
    }
}
