using System.IO;
using ECommon.Utilities;

namespace EQueue.Broker.Storage
{
    public abstract class LogRecord
    {
        public byte Type { get; private set; }
        public byte Version { get; private set; }
        public long LogPosition { get; set; }

        protected LogRecord(byte type)
        {
            Ensure.Positive(type, "type");

            Type = type;
        }
        protected LogRecord(byte type, byte version, long logPosition)
        {
            Ensure.Positive(type, "type");
            Ensure.Positive(version, "version");
            Ensure.Nonnegative(logPosition, "logPosition");

            Type = type;
            Version = version;
            LogPosition = logPosition;
        }

        public virtual void WriteTo(BinaryWriter writer)
        {
            writer.Write(Version);
            writer.Write(LogPosition);
        }
        public virtual void ParseFrom(BinaryReader reader)
        {
            Version = reader.ReadByte();
            LogPosition = reader.ReadInt64();

            Ensure.Positive(Version, "Version");
            Ensure.Nonnegative(LogPosition, "LogPosition");
        }

        public int GetSizeWithLengthPrefixAndSuffix()
        {
            using (var memoryStream = new MemoryStream())
            {
                WriteTo(new BinaryWriter(memoryStream));
                return 8 + (int)memoryStream.Length;
            }
        }

        internal void WriteWithLengthPrefixAndSuffixTo(BinaryWriter writer)
        {
            using (var memoryStream = new MemoryStream())
            {
                WriteTo(new BinaryWriter(memoryStream));
                var length = (int)memoryStream.Length;
                writer.Write(length);
                writer.Write(memoryStream.GetBuffer(), 0, (int)memoryStream.Length);
                writer.Write(length);
            }
        }
    }
}
