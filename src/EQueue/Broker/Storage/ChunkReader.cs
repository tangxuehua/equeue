using System;
using System.IO;
using ECommon.Utilities;
using EQueue.Broker.Storage.LogRecords;

namespace EQueue.Broker.Storage
{
    public class ChunkReader
    {
        private readonly ChunkManager _chunkManager;
        private readonly ChunkWriter _chunkWriter;

        public ChunkReader(ChunkManager chunkManager, ChunkWriter chunkWriter)
        {
            Ensure.NotNull(chunkManager, "chunkManager");
            Ensure.NotNull(chunkWriter, "chunkWriter");

            _chunkManager = chunkManager;
            _chunkWriter = chunkWriter;
        }

        public T TryReadAt<T>(long position, Func<byte[], T> readRecordFunc, bool autoCache = true) where T : class, ILogRecord
        {
            var lastChunk = _chunkWriter.CurrentChunk;
            var maxPosition = lastChunk.GlobalDataPosition;
            if (position >= maxPosition)
            {
                return null;
            }

            var chunkNum = _chunkManager.GetChunkNum(position);
            var chunk = _chunkManager.GetChunk(chunkNum);
            if (chunk == null)
            {
                throw new ChunkNotExistException(position, chunkNum);
            }

            var localPosition = chunk.ChunkHeader.GetLocalDataPosition(position);
            return chunk.TryReadAt(localPosition, readRecordFunc, autoCache);
        }
        public BufferLogRecord TryReadRecordBufferAt(long position)
        {
            return TryReadAt(position, recordBuffer =>
            {
                var record = new BufferLogRecord();
                record.ReadFrom(recordBuffer);
                return record;
            });
        }
    }
}
