using System;
using System.IO;
using ECommon.Utilities;

namespace EQueue.Broker.Storage
{
    public class ChunkHeader
    {
        public const int Size = 128;
        public readonly byte Version;
        public readonly int ChunkDataSize;
        public readonly int ChunkStartNumber;
        public readonly int ChunkEndNumber;
        public readonly long ChunkDataStartPosition;
        public readonly long ChunkDataEndPosition;
        public readonly Guid ChunkId;

        public ChunkHeader(byte version, int chunkDataSize, int chunkStartNumber, int chunkEndNumber, Guid chunkId)
        {
            Ensure.Nonnegative(version, "version");
            Ensure.Positive(chunkDataSize, "chunkSize");
            Ensure.Nonnegative(chunkStartNumber, "chunkStartNumber");
            Ensure.Nonnegative(chunkEndNumber, "chunkEndNumber");
            if (chunkStartNumber > chunkEndNumber)
            {
                throw new ArgumentOutOfRangeException("chunkStartNumber", "chunkStartNumber is greater than chunkEndNumber.");
            }

            Version = version;
            ChunkDataSize = chunkDataSize;
            ChunkStartNumber = chunkStartNumber;
            ChunkEndNumber = chunkEndNumber;
            ChunkId = chunkId;

            ChunkDataStartPosition = ChunkStartNumber * (long)ChunkDataSize;
            ChunkDataEndPosition = (ChunkEndNumber + 1) * (long)ChunkDataSize;
        }

        public byte[] AsByteArray()
        {
            var array = new byte[Size];
            using (var stream = new MemoryStream(array))
            {
                using (var writer = new BinaryWriter(stream))
                {
                    writer.Write(Version);
                    writer.Write(ChunkDataSize);
                    writer.Write(ChunkStartNumber);
                    writer.Write(ChunkEndNumber);
                    writer.Write(ChunkId.ToByteArray());
                }
            }
            return array;
        }
        public static ChunkHeader FromStream(Stream stream)
        {
            var reader = new BinaryReader(stream);
            var version = reader.ReadByte();
            var chunkDataSize = reader.ReadInt32();
            var chunkStartNumber = reader.ReadInt32();
            var chunkEndNumber = reader.ReadInt32();
            var chunkId = new Guid(reader.ReadBytes(16));
            return new ChunkHeader(version, chunkDataSize, chunkStartNumber, chunkEndNumber, chunkId);
        }

        public int GetLocalDataPosition(long globalDataPosition)
        {
            if (globalDataPosition < ChunkDataStartPosition || globalDataPosition > ChunkDataEndPosition)
            {
                throw new Exception(string.Format("globalDataPosition {0} is out of chunk data positions [{1}, {2}].", globalDataPosition, ChunkDataStartPosition, ChunkDataEndPosition));
            }
            return (int)(globalDataPosition - ChunkDataStartPosition);
        }

        public override string ToString()
        {
            return string.Format("[Version:{0}, ChunkId:{1}, ChunkStartNumber:{2}, ChunkEndNumber:{3}, ChunkDataSize:{4}, DataStartPosition:{5}, DataEndPosition:{6}]",
                                 Version,
                                 ChunkId,
                                 ChunkStartNumber,
                                 ChunkEndNumber,
                                 ChunkDataSize,
                                 ChunkDataStartPosition,
                                 ChunkDataEndPosition);
        }
    }
}
