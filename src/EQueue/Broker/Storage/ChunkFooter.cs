using System.IO;
using ECommon.Utilities;

namespace EQueue.Broker.Storage
{
    public class ChunkFooter
    {
        public const int Size = 128;
        public readonly bool IsCompleted;
        public readonly int DataPosition;

        public ChunkFooter(bool isCompleted, int dataPosition)
        {
            Ensure.Nonnegative(dataPosition, "dataPosition");
            IsCompleted = isCompleted;
            DataPosition = dataPosition;
        }

        public byte[] AsByteArray()
        {
            var array = new byte[Size];
            using (var stream = new MemoryStream(array))
            {
                using (var writer = new BinaryWriter(stream))
                {
                    var flags = (byte)(IsCompleted ? 1 : 0);
                    writer.Write(flags);
                    writer.Write(DataPosition);
                }
            }
            return array;
        }

        public static ChunkFooter FromStream(Stream stream)
        {
            var reader = new BinaryReader(stream);
            var flags = reader.ReadByte();
            var isCompleted = (flags & 1) != 0;
            var dataPosition = reader.ReadInt32();
            return new ChunkFooter(isCompleted, dataPosition);
        }
    }
}
