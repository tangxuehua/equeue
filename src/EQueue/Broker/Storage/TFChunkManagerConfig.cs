using System;
using ECommon.Utilities;

namespace EQueue.Broker.Storage
{
    public class TFChunkManagerConfig
    {
        public readonly string BasePath;
        public readonly IFileNamingStrategy FileNamingStrategy;
        public readonly int ChunkDataSize;
        public readonly int ChunkDataUnitSize;
        public readonly int ChunkDataCount;
        public readonly int FlushChunkIntervalMilliseconds;
        public readonly int ChunkReaderCount;
        public readonly int MaxLogRecordSize;

        public TFChunkManagerConfig(string basePath,
                               IFileNamingStrategy fileNamingStrategy,
                               int chunkDataSize,
                               int chunkDataUnitSize,
                               int chunkDataCount,
                               int flushChunkIntervalMilliseconds,
                               int chunkReaderCount,
                               int maxLogRecordSize)
        {
            Ensure.NotNullOrEmpty(basePath, "basePath");
            Ensure.NotNull(fileNamingStrategy, "fileNamingStrategy");
            Ensure.Nonnegative(chunkDataSize, "chunkDataSize");
            Ensure.Nonnegative(chunkDataUnitSize, "chunkDataUnitSize");
            Ensure.Nonnegative(chunkDataCount, "chunkDataCount");
            Ensure.Positive(flushChunkIntervalMilliseconds, "flushChunkIntervalMilliseconds");
            Ensure.Positive(maxLogRecordSize, "maxLogRecordSize");

            if (chunkDataSize <= 0 && (chunkDataUnitSize <= 0 || chunkDataCount <= 0))
            {
                throw new ArgumentException(string.Format("Invalid chunk data size arugment. chunkDataSize: {0}, chunkDataUnitSize: {1}, chunkDataCount: {2}", chunkDataSize, chunkDataUnitSize, chunkDataCount));
            }

            BasePath = basePath;
            FileNamingStrategy = fileNamingStrategy;
            ChunkDataSize = chunkDataSize;
            ChunkDataUnitSize = chunkDataUnitSize;
            ChunkDataCount = chunkDataCount;
            FlushChunkIntervalMilliseconds = flushChunkIntervalMilliseconds;
            ChunkReaderCount = chunkReaderCount;
            MaxLogRecordSize = maxLogRecordSize;
        }

        public int GetChunkDataSize()
        {
            if (ChunkDataSize > 0)
            {
                return ChunkDataSize;
            }
            return ChunkDataUnitSize * ChunkDataCount;
        }

        public static TFChunkManagerConfig Create(string basePath, string chunkFilePrefix, int chunkDataSize, int chunkDataUnitSize, int chunkDataCount, int flushChunkIntervalMilliseconds)
        {
            return new TFChunkManagerConfig(
                basePath,
                new DefaultFileNamingStrategy(chunkFilePrefix),
                chunkDataSize,
                chunkDataUnitSize,
                chunkDataCount,
                flushChunkIntervalMilliseconds,
                Environment.ProcessorCount * 2,
                4 * 1024 * 1024);
        }
    }
}
