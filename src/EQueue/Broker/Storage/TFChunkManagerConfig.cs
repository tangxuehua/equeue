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
        public readonly bool ForceCacheChunk;
        public readonly int MessageChunkCacheMaxPercent;
        public readonly int InitialCacheChunkCount;
        public readonly int ChunkInactiveTimeMaxSeconds;

        public TFChunkManagerConfig(string basePath,
                               IFileNamingStrategy fileNamingStrategy,
                               int chunkDataSize,
                               int chunkDataUnitSize,
                               int chunkDataCount,
                               int flushChunkIntervalMilliseconds,
                               int chunkReaderCount,
                               int maxLogRecordSize,
                               bool forceCacheChunk,
                               int messageChunkCacheMaxPercent,
                               int initialCacheChunkCount,
                               int chunkInactiveTimeMaxSeconds)
        {
            Ensure.NotNullOrEmpty(basePath, "basePath");
            Ensure.NotNull(fileNamingStrategy, "fileNamingStrategy");
            Ensure.Nonnegative(chunkDataSize, "chunkDataSize");
            Ensure.Nonnegative(chunkDataUnitSize, "chunkDataUnitSize");
            Ensure.Nonnegative(chunkDataCount, "chunkDataCount");
            Ensure.Positive(flushChunkIntervalMilliseconds, "flushChunkIntervalMilliseconds");
            Ensure.Positive(maxLogRecordSize, "maxLogRecordSize");
            Ensure.Positive(messageChunkCacheMaxPercent, "messageChunkCacheMaxPercent");
            Ensure.Nonnegative(initialCacheChunkCount, "initialCacheChunkCount");
            Ensure.Nonnegative(chunkInactiveTimeMaxSeconds, "chunkInactiveTimeMaxSeconds");

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
            ForceCacheChunk = forceCacheChunk;
            MessageChunkCacheMaxPercent = messageChunkCacheMaxPercent;
            InitialCacheChunkCount = initialCacheChunkCount;
            ChunkInactiveTimeMaxSeconds = chunkInactiveTimeMaxSeconds;
        }

        public int GetChunkDataSize()
        {
            if (ChunkDataSize > 0)
            {
                return ChunkDataSize;
            }
            return ChunkDataUnitSize * ChunkDataCount;
        }

        public static TFChunkManagerConfig Create(string basePath, string chunkFilePrefix, int chunkDataSize, int chunkDataUnitSize, int chunkDataCount, int flushChunkIntervalMilliseconds, bool forceCacheChunk, int messageChunkCacheMaxPercent, int initialCacheChunkCount, int chunkInactiveTimeMaxSeconds)
        {
            return new TFChunkManagerConfig(
                basePath,
                new DefaultFileNamingStrategy(chunkFilePrefix),
                chunkDataSize,
                chunkDataUnitSize,
                chunkDataCount,
                flushChunkIntervalMilliseconds,
                Environment.ProcessorCount * 2,
                4 * 1024 * 1024,
                forceCacheChunk,
                messageChunkCacheMaxPercent,
                initialCacheChunkCount,
                chunkInactiveTimeMaxSeconds);
        }
    }
}
