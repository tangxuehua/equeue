using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Scheduling;
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
        private readonly IScheduleService _scheduleService;
        private readonly string _uncacheChunkTaskName;
        private int _nextChunkNumber;

        public string Name { get; private set; }
        public TFChunkManagerConfig Config { get { return _config; } }
        public string ChunkPath { get { return _chunkPath; } }

        public TFChunkManager(string name, TFChunkManagerConfig config, string relativePath = null)
        {
            Ensure.NotNull(name, "name");
            Ensure.NotNull(config, "config");

            Name = name;
            _config = config;
            if (string.IsNullOrEmpty(relativePath))
            {
                _chunkPath = _config.BasePath;
            }
            else
            {
                _chunkPath = Path.Combine(_config.BasePath, relativePath);
            }
            _chunks = new ConcurrentDictionary<int, TFChunk>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _uncacheChunkTaskName = string.Format("{0}.{1}.UncacheChunks", Name, this.GetType().Name);
            _scheduleService.StartTask(_uncacheChunkTaskName, UncacheChunks, 1000, 5000);
        }

        public void Load<T>(Func<int, BinaryReader, T> readRecordFunc) where T : ILogRecord
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
                    var cachedChunkCount = 0;
                    for (var i = files.Length - 2; i >= 0; i--)
                    {
                        var chunk = TFChunk.FromCompletedFile(files[i], _config);
                        if (cachedChunkCount < _config.InitialCacheChunkCount)
                        {
                            chunk.TryCacheInMemory();
                            cachedChunkCount++;
                        }
                        AddChunk(chunk);
                    }
                    AddChunk(TFChunk.FromOngoingFile(files[files.Length - 1], _config, readRecordFunc));
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
                var chunk = TFChunk.CreateNew(chunkFileName, chunkNumber, _config);

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
            if (_chunks.ContainsKey(chunkNum))
            {
                return _chunks[chunkNum];
            }
            return null;
        }
        public bool RemoveChunk(TFChunk chunk)
        {
            lock (_chunksLocker)
            {
                if (_chunks.Remove(chunk.ChunkHeader.ChunkNumber))
                {
                    try
                    {
                        chunk.Delete();
                    }
                    catch (Exception ex)
                    {
                        _logger.Error(string.Format("Delete chunk {0} has exception.", chunk), ex);
                    }
                    return true;
                }
                return false;
            }
        }

        public void Dispose()
        {
            Close();
        }
        public void Close()
        {
            lock (_chunksLocker)
            {
                _scheduleService.StopTask(_uncacheChunkTaskName);
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
        private void UncacheChunks()
        {
            var chunks = _chunks.Values.Where(x => x.IsCompleted && !x.IsMemoryChunk && x.HasCachedChunk).OrderBy(x => x.ChunkHeader.ChunkNumber).ToList();
            var count = chunks.Count() - _config.InitialCacheChunkCount;

            if (count > 0)
            {
                for (var i = 0; i < count; i++)
                {
                    var chunk = chunks[i];
                    if ((DateTime.Now - chunk.LastActiveTime).TotalSeconds >= _config.ChunkInactiveTimeMaxSeconds)
                    {
                        chunk.UnCacheFromMemory();
                    }
                }
            }
        }
    }
}
