namespace EQueue.Broker.Storage
{
    public static class Consts
    {
        public const int TFChunkInitialReaderCount = 5;
        public const int TFChunkMaxReaderCount = 10;
        public const int MaxLogRecordSize = 16 * 1024 * 1024;
        public const int ChunkSize = 256 * 1024 * 1024;
        public const int ChunksCacheCount = 2;
    }
}
