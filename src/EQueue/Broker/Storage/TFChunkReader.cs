using ECommon.Utilities;

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

        public RecordReadResult TryReadAt(long position)
        {
            var maxPosition = _chunkWriter.CurrentChunk.GlobalDataPosition;
            if (position >= maxPosition)
            {
                return RecordReadResult.Failure;
            }

            var chunk = _chunkManager.GetChunkFor(position);
            if (chunk == null)
            {
                return RecordReadResult.Failure;
            }

            var localPosition = chunk.ChunkHeader.GetLocalDataPosition(position);
            return chunk.TryReadAt(localPosition);
        }
        public RecordBufferReadResult TryReadRecordBufferAt(long position)
        {
            var maxPosition = _chunkWriter.CurrentChunk.GlobalDataPosition;
            if (position >= maxPosition)
            {
                return RecordBufferReadResult.Failure;
            }

            var chunk = _chunkManager.GetChunkFor(position);
            if (chunk == null)
            {
                return RecordBufferReadResult.Failure;
            }

            var localPosition = chunk.ChunkHeader.GetLocalDataPosition(position);
            return chunk.TryReadRecordBufferAt(localPosition);
        }
    }
}
