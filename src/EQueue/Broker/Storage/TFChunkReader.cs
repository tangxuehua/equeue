using System;
using System.IO;
using ECommon.Utilities;
using EQueue.Broker.Storage.LogRecords;

namespace EQueue.Broker.Storage
{
    public class TFChunkReader
    {
        private readonly TFChunkManager _chunkManager;
        private readonly TFChunkWriter _chunkWriter;

        public TFChunkReader(TFChunkManager chunkManager, TFChunkWriter chunkWriter)
        {
            Ensure.NotNull(chunkManager, "chunkManager");
            Ensure.NotNull(chunkWriter, "chunkWriter");

            _chunkManager = chunkManager;
            _chunkWriter = chunkWriter;
        }

        public T TryReadAt<T>(long position, Func<int, BinaryReader, T> readRecordFunc) where T : class, ILogRecord
        {
            var lastChunk = _chunkWriter.CurrentChunk;
            var maxPosition = lastChunk.GlobalDataPosition;
            if (position >= maxPosition)
            {
                throw new ChunkReadException(
                    string.Format("Cannot read record after the max global data position, data position: {0}, max global data position: {1}, chunk: {2}.",
                                  position, maxPosition, lastChunk));
            }

            var chunk = _chunkManager.GetChunkFor(position);
            if (chunk == null)
            {
                throw new ChunkNotExistException(string.Format("Cannot get chunk by position: {0}", position));
            }

            var localPosition = chunk.ChunkHeader.GetLocalDataPosition(position);
            return chunk.TryReadAt(localPosition, readRecordFunc);
        }
        public BufferLogRecord TryReadRecordBufferAt(long position)
        {
            return TryReadAt(position, (length, reader) =>
            {
                var record = new BufferLogRecord();
                record.ReadFrom(length, reader);
                return record;
            });
        }
    }
}
