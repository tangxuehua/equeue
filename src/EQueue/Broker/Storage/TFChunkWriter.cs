using System;
using ECommon.Utilities;

namespace EQueue.Broker.Storage
{
    public class TFChunkWriter
    {
        public ICheckpoint Checkpoint { get { return _writerCheckpoint; } }
        public TFChunk CurrentChunk { get { return _currentChunk; } }

        private readonly TFChunkDb _chunkDb;
        private readonly ICheckpoint _writerCheckpoint;

        private TFChunk _currentChunk;

        public TFChunkWriter(TFChunkDb chunkDb)
        {
            Ensure.NotNull(chunkDb, "chunkDb");

            _chunkDb = chunkDb;
            _writerCheckpoint = chunkDb.Config.WriterCheckpoint;
        }

        public void Open()
        {
            _currentChunk = _chunkDb.Manager.GetChunkFor(_writerCheckpoint.Read());
            if (_currentChunk == null)
                throw new InvalidOperationException("No chunk given for existing position.");
        }

        public void Write(ILogRecord record)
        {
            if (!WriteInternal(record))
            {
                record.LogPosition = _writerCheckpoint.ReadNonFlushed();
                if (!WriteInternal(record))
                {
                    throw new ChunkWriteException(_currentChunk.ToString());
                }
            }
        }

        public void CompleteChunk()
        {
            var chunk = _currentChunk;
            _currentChunk = null; // in case creation of new chunk fails, we shouldn't use completed chunk for write

            chunk.Complete();

            _writerCheckpoint.Write(chunk.ChunkHeader.ChunkDataEndPosition);
            _writerCheckpoint.Flush();

            _currentChunk = _chunkDb.Manager.AddNewChunk();
        }

        public void Dispose()
        {
            Close();
        }

        public void Close()
        {
            Flush();
        }

        public void Flush()
        {
            if (_currentChunk == null) // the last chunk allocation failed
                return;
            _currentChunk.Flush();
            _writerCheckpoint.Flush();
        }

        private bool WriteInternal(ILogRecord record)
        {
            var result = _currentChunk.TryAppend(record);
            if (result.Success)
            {
                _writerCheckpoint.Write(result.NewPosition + _currentChunk.ChunkHeader.ChunkDataStartPosition);
            }
            else
            {
                CompleteChunk(); // complete updates checkpoint internally
            }
            return result.Success;
        }
    }
}
