using System;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Utilities;

namespace EQueue.Broker.Storage
{
    public class TFChunkManager : IDisposable
    {
        private static readonly ILogger _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(typeof(TFChunkManager));
        private readonly object _chunksLocker = new object();
        private readonly TFChunkManagerConfig _config;
        private readonly TFChunk[] _chunks;
        private volatile int _chunksCount;

        public TFChunkManagerConfig Config { get { return _config; } }
        public int ChunksCount { get { return _chunksCount; } }

        public TFChunkManager(TFChunkManagerConfig config)
        {
            Ensure.NotNull(config, "config");
            Ensure.Positive(config.MaxChunksCount, "MaxChunksCount");
            _config = config;
            _chunks = new TFChunk[config.MaxChunksCount];
        }

        public void Load()
        {
            lock (_chunksLocker)
            {
                var files = _config.FileNamingStrategy.GetAllFiles(_config.Path);

                if (files.Length > 0)
                {
                    for (var i = 0; i < files.Length - 1; i++)
                    {
                        AddChunk(TFChunk.FromCompletedFile(files[i], _config));
                    }
                    AddChunk(TFChunk.FromOngoingFile(files[files.Length - 1], _config));
                }
            }
        }
        public TFChunk AddNewChunk()
        {
            lock (_chunksLocker)
            {
                var chunkNumber = _chunksCount;
                var chunkFileName = _config.FileNamingStrategy.GetFileNameFor(_config.Path, chunkNumber);
                var chunk = TFChunk.CreateNew(chunkFileName, chunkNumber, _config);

                AddChunk(chunk);

                return chunk;
            }
        }
        public TFChunk GetLastChunk()
        {
            lock (_chunksLocker)
            {
                if (_chunksCount == 0)
                {
                    AddNewChunk();
                }
                return _chunks[_chunksCount - 1];
            }
        }
        public TFChunk GetChunkFor(long dataPosition)
        {
            var chunkNum = (int)(dataPosition / _config.ChunkDataSize);
            if (chunkNum < 0 || chunkNum >= _chunksCount)
            {
                throw new ArgumentOutOfRangeException("dataPosition", string.Format("DataPosition {0} doesn't have corresponding chunk.", dataPosition));
            }

            var chunk = _chunks[chunkNum];
            if (chunk == null)
            {
                throw new Exception(string.Format("Requested chunk for dataPosition {0}, which is not present in TFChunkManager.", dataPosition));
            }
            return chunk;
        }
        public TFChunk GetChunk(int chunkNum)
        {
            if (chunkNum < 0 || chunkNum >= _chunksCount)
            {
                throw new ArgumentOutOfRangeException("chunkNum", string.Format("Chunk #{0} isn't present in TFChunkManager.", chunkNum));
            }

            var chunk = _chunks[chunkNum];
            if (chunk == null)
            {
                throw new Exception(string.Format("Requested chunk #{0}, which is not present in TFChunkManager.", chunkNum));
            }

            return chunk;
        }

        public void Dispose()
        {
            Close();
        }
        public void Close()
        {
            lock (_chunksLocker)
            {
                _logger.Info("TFChunkManager is closing.");
                for (int i = 0; i < _chunksCount; ++i)
                {
                    if (_chunks[i] != null)
                    {
                        _chunks[i].Close();
                    }
                }
                _logger.Info("TFChunkManager closed.");
            }
        }

        private void AddChunk(TFChunk chunk)
        {
            _chunks[chunk.ChunkHeader.ChunkNumber] = chunk;
            _chunksCount++;
        }
    }
}
