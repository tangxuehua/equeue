using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
        private readonly IList<TFChunk> _chunks;
        private readonly string _chunkPath;
        private volatile int _nextChunkNumber;

        public TFChunkManagerConfig Config { get { return _config; } }
        public string ChunkPath { get { return _chunkPath; } }

        public TFChunkManager(TFChunkManagerConfig config, string relativePath = null)
        {
            Ensure.NotNull(config, "config");
            _config = config;
            if (string.IsNullOrEmpty(relativePath))
            {
                _chunkPath = _config.BasePath;
            }
            else
            {
                _chunkPath = Path.Combine(_config.BasePath, relativePath);
            }
            _chunks = new List<TFChunk>();
        }

        public void Load()
        {
            lock (_chunksLocker)
            {
                if (!Directory.Exists(_chunkPath))
                {
                    Directory.CreateDirectory(_chunkPath);
                }
                var files = _config.FileNamingStrategy.GetAllFiles(_chunkPath);

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
                var chunkNumber = _nextChunkNumber;
                var chunkFileName = _config.FileNamingStrategy.GetFileNameFor(_chunkPath, chunkNumber);
                var chunk = TFChunk.CreateNew(chunkFileName, chunkNumber, _config);

                AddChunk(chunk);

                return chunk;
            }
        }
        public TFChunk GetLastChunk()
        {
            lock (_chunksLocker)
            {
                if (_chunks.Count == 0)
                {
                    AddNewChunk();
                }
                return _chunks.Last();
            }
        }
        public TFChunk GetChunkFor(long dataPosition)
        {
            var chunkNum = (int)(dataPosition / _config.ChunkDataSize);
            return GetChunk(chunkNum);
        }
        public TFChunk GetChunk(int chunkNum)
        {
            return _chunks.FirstOrDefault(x => x.ChunkHeader.ChunkNumber == chunkNum);
        }

        public void Dispose()
        {
            Close();
        }
        public void Close()
        {
            lock (_chunksLocker)
            {
                foreach (var chunk in _chunks)
                {
                    try
                    {
                        chunk.Close();
                    }
                    catch (Exception ex)
                    {
                        _logger.Error(string.Format("Chunk {0} close failed.", chunk), ex);
                    }
                }
            }
        }

        private void AddChunk(TFChunk chunk)
        {
            _chunks.Add(chunk);
            _nextChunkNumber = chunk.ChunkHeader.ChunkNumber + 1;
        }
    }
}
