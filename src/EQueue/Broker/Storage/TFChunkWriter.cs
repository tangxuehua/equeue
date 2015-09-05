using System;
using System.Threading;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Utilities;

namespace EQueue.Broker.Storage
{
    public class TFChunkWriter
    {
        public ICheckpoint Checkpoint { get { return _writerCheckpoint; } }
        public TFChunk CurrentChunk { get { return _currentChunk; } }

        private readonly TFChunkDb _chunkDb;
        private readonly ICheckpoint _writerCheckpoint;
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;
        private readonly object _lockObj = new object();
        private bool _isClosed = false;

        private TFChunk _currentChunk;

        public TFChunkWriter(TFChunkDb chunkDb)
        {
            Ensure.NotNull(chunkDb, "chunkDb");

            _chunkDb = chunkDb;
            _writerCheckpoint = chunkDb.Config.WriterCheckpoint;
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(typeof(TFChunkWriter).FullName);
        }

        public void Open()
        {
            _currentChunk = _chunkDb.Manager.GetChunkFor(_writerCheckpoint.Read());
            if (_currentChunk == null)
            {
                throw new InvalidOperationException("No chunk given for existing position.");
            }
            _scheduleService.StartTask("TFChunkWriter.Flush", Flush, 1000, _chunkDb.Config.FlushChunkIntervalMilliseconds);
        }
        public void Write(ILogRecord record)
        {
            if (!WriteInternal(record))
            {
                if (_isClosed) return;

                record.LogPosition = _writerCheckpoint.ReadNonFlushed();
                if (!WriteInternal(record))
                {
                    if (_isClosed) return;

                    throw new ChunkWriteException(_currentChunk.ToString());
                }
            }
        }
        public void Close()
        {
            lock (_lockObj)
            {
                _scheduleService.StopTask("TFChunkWriter.Flush");
                _isClosed = true;
            }
        }

        long _flushCount;
        private void Flush()
        {
            lock (_lockObj)
            {
                if (_isClosed) return;
                if (_currentChunk == null) return;

                _currentChunk.Flush();
                _writerCheckpoint.Flush();

                var globalDataPosition = _currentChunk.ChunkHeader.ChunkDataStartPosition + _currentChunk.DataPosition;
                var writeCheckpoint = _writerCheckpoint.Read();

                var flushCount = Interlocked.Increment(ref _flushCount);
                if (flushCount % 100 == 0)
                {
                    _logger.InfoFormat("Flushed chunk: {0}, header: {1}, dataPosition: {2}, globalDataPosition: {3}, writeCheckpoint: {4}, unFlushedDataSize: {5}, flushCount: {6}",
                        _currentChunk,
                        _currentChunk.ChunkHeader,
                        _currentChunk.DataPosition,
                        globalDataPosition,
                        writeCheckpoint,
                        globalDataPosition - writeCheckpoint,
                        flushCount);
                }
            }
        }

        private bool WriteInternal(ILogRecord record)
        {
            lock (_lockObj)
            {
                if (_isClosed) return false;

                if (_currentChunk == null)
                {
                    throw new InvalidOperationException("Cannot write record as the current chunk is null.");
                }

                var result = _currentChunk.TryAppend(record);
                if (result.Success)
                {
                    _writerCheckpoint.Write(result.NewPosition + _currentChunk.ChunkHeader.ChunkDataStartPosition);
                }
                else
                {
                    CompleteChunk();
                }
                return result.Success;
            }
        }
        private void CompleteChunk()
        {
            var chunk = _currentChunk;
            _currentChunk = null; // in case creation of new chunk fails, we shouldn't use completed chunk for write

            chunk.Complete();

            _writerCheckpoint.Write(chunk.ChunkHeader.ChunkDataEndPosition);
            _writerCheckpoint.Flush();

            _currentChunk = _chunkDb.Manager.AddNewChunk();
        }
    }
}
