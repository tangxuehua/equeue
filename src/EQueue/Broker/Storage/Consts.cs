namespace EQueue.Broker.Storage
{
    public static class Consts
    {
        public const int MaxChunksCount = 100000;
        public const int TFChunkReaderCount = 10;
        public const int MaxLogRecordSize = 4 * 1024 * 1024;
        public const int ChunkDataSize = 256 * 1024 * 1024;
        public const int FlushChunkIntervalMilliseconds = 100;
    }
}
