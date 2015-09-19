using System.IO;
using ECommon.Utilities;

namespace EQueue.Broker.Storage
{
    public class TFChunkManagerConfig
    {
        public readonly string Path;
        public readonly IFileNamingStrategy FileNamingStrategy;
        public readonly int MaxChunksCount;
        public readonly int ChunkDataSize;
        public readonly int ChunkDataUnitSize;
        public readonly int ChunkDataCount;
        public readonly int FlushChunkIntervalMilliseconds;
        public readonly int ChunkReaderCount;

        public TFChunkManagerConfig(string path,
                               IFileNamingStrategy fileNamingStrategy,
                               int maxChunksCount,
                               int chunkDataSize,
                               int chunkDataUnitSize,
                               int chunkDataCount,
                               int flushChunkIntervalMilliseconds,
                               int chunkReaderCount)
        {
            Ensure.NotNullOrEmpty(path, "path");
            Ensure.NotNull(fileNamingStrategy, "fileNamingStrategy");
            Ensure.Positive(maxChunksCount, "maxChunksCount");
            Ensure.Positive(chunkDataSize, "chunkDataSize");
            Ensure.Nonnegative(chunkDataUnitSize, "chunkDataUnitSize");
            Ensure.Nonnegative(chunkDataCount, "chunkDataCount");
            Ensure.Positive(flushChunkIntervalMilliseconds, "flushChunkIntervalMilliseconds");

            Path = path;
            FileNamingStrategy = fileNamingStrategy;
            MaxChunksCount = maxChunksCount;
            ChunkDataSize = chunkDataSize;
            ChunkDataUnitSize = chunkDataUnitSize;
            ChunkDataCount = chunkDataCount;
            FlushChunkIntervalMilliseconds = flushChunkIntervalMilliseconds;
            ChunkReaderCount = chunkReaderCount;
        }
    }
}
