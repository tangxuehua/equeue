using ECommon.Utilities;

namespace EQueue.Broker.Storage
{
    public class TFChunkDbConfig
    {
        public readonly string Path;
        public readonly int ChunkDataSize;
        public readonly long MaxChunksCacheSize;
        public readonly int FlushChunkIntervalMilliseconds;
        public readonly ICheckpoint WriterCheckpoint;
        public readonly IFileNamingStrategy FileNamingStrategy;
        public readonly bool InMemDb;

        public TFChunkDbConfig(string path,
                               IFileNamingStrategy fileNamingStrategy,
                               int chunkDataSize,
                               long maxChunksCacheSize,
                               int flushChunkIntervalMilliseconds,
                               ICheckpoint writerCheckpoint,
                               bool inMemDb = false)
        {
            Ensure.NotNullOrEmpty(path, "path");
            Ensure.NotNull(fileNamingStrategy, "fileNamingStrategy");
            Ensure.Positive(chunkDataSize, "chunkDataSize");
            Ensure.Positive(flushChunkIntervalMilliseconds, "flushChunkIntervalMilliseconds");
            Ensure.Nonnegative(maxChunksCacheSize, "maxChunksCacheSize");
            Ensure.NotNull(writerCheckpoint, "writerCheckpoint");

            Path = path;
            ChunkDataSize = chunkDataSize;
            MaxChunksCacheSize = maxChunksCacheSize;
            FlushChunkIntervalMilliseconds = flushChunkIntervalMilliseconds;
            WriterCheckpoint = writerCheckpoint;
            FileNamingStrategy = fileNamingStrategy;
            InMemDb = inMemDb;
        }
    }
}
