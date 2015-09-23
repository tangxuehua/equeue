using System;
using System.Collections.Concurrent;
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
        private readonly IDictionary<int, TFChunk> _chunks;
        private readonly string _chunkPath;
        private readonly Func<BinaryReader, ILogRecord> _readRecordFunc;
        private int _nextChunkNumber;

        public string Name { get; private set; }
        public TFChunkManagerConfig Config { get { return _config; } }
        public string ChunkPath { get { return _chunkPath; } }

        public TFChunkManager(string name, TFChunkManagerConfig config, Func<BinaryReader, ILogRecord> readRecordFunc, string relativePath = null)
        {
            Ensure.NotNull(name, "name");
            Ensure.NotNull(config, "config");

            Name = name;
            _config = config;
            _readRecordFunc = readRecordFunc;
            if (string.IsNullOrEmpty(relativePath))
            {
                _chunkPath = _config.BasePath;
            }
            else
            {
                _chunkPath = Path.Combine(_config.BasePath, relativePath);
            }
            _chunks = new ConcurrentDictionary<int, TFChunk>();
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
                        AddChunk(TFChunk.FromCompletedFile(files[i], _config, _readRecordFunc));
                    }
                    AddChunk(TFChunk.FromOngoingFile(files[files.Length - 1], _config, _readRecordFunc));
                }
            }
        }
        public IList<TFChunk> GetAllChunks()
        {
            return _chunks.Values.ToList();
        }
        public TFChunk AddNewChunk()
        {
            lock (_chunksLocker)
            {
                var chunkNumber = _nextChunkNumber;
                var chunkFileName = _config.FileNamingStrategy.GetFileNameFor(_chunkPath, chunkNumber);
                var chunk = TFChunk.CreateNew(chunkFileName, chunkNumber, _config, _readRecordFunc);

                AddChunk(chunk);

                return chunk;
            }
        }
        public TFChunk GetFirstChunk()
        {
            lock (_chunksLocker)
            {
                if (_chunks.Count == 0)
                {
                    AddNewChunk();
                }
                var minChunkNum = _chunks.Keys.Min();
                return _chunks[minChunkNum];
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
                return _chunks[_nextChunkNumber - 1];
            }
        }
        public TFChunk GetChunkFor(long dataPosition)
        {
            var chunkNum = (int)(dataPosition / _config.GetChunkDataSize());
            return GetChunk(chunkNum);
        }
        public TFChunk GetChunk(int chunkNum)
        {
            return _chunks[chunkNum];
        }
        public void RemoveChunk(TFChunk chunk)
        {
            chunk.MarkForDeletion();
            _logger.InfoFormat("Chunk {0} is deleted.", chunk);
        }

        public void Dispose()
        {
            Close();
        }
        public void Close()
        {
            lock (_chunksLocker)
            {
                foreach (var chunk in _chunks.Values)
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
            _chunks.Add(chunk.ChunkHeader.ChunkNumber, chunk);
            _nextChunkNumber = chunk.ChunkHeader.ChunkNumber + 1;
        }
    }
}
