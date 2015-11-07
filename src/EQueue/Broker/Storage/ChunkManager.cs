using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Utilities;
using EQueue.Broker.Storage.LogRecords;

namespace EQueue.Broker.Storage
{
    public class ChunkManager : IDisposable
    {
        private static readonly ILogger _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(typeof(ChunkManager));
        private readonly object _lockObj = new object();
        private readonly ChunkManagerConfig _config;
        private readonly IDictionary<int, Chunk> _chunks;
        private readonly string _chunkPath;
        private readonly IScheduleService _scheduleService;
        private int _nextChunkNumber;
        private int _uncachingChunks;
        private int _isCachingNextChunk;

        public string Name { get; private set; }
        public ChunkManagerConfig Config { get { return _config; } }
        public string ChunkPath { get { return _chunkPath; } }

        public ChunkManager(string name, ChunkManagerConfig config, string relativePath = null)
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
            _chunks = new ConcurrentDictionary<int, Chunk>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
        }

        public void Load<T>(Func<byte[], T> readRecordFunc) where T : ILogRecord
        {
            lock (_lockObj)
            {
                if (!Directory.Exists(_chunkPath))
                {
                    Directory.CreateDirectory(_chunkPath);
                }

                var tempFiles = _config.FileNamingStrategy.GetTempFiles(_chunkPath);
                foreach (var file in tempFiles)
                {
                    File.SetAttributes(file, FileAttributes.Normal);
                    File.Delete(file);
                }

                var files = _config.FileNamingStrategy.GetChunkFiles(_chunkPath);
                if (files.Length > 0)
                {
                    var cachedChunkCount = 0;
                    for (var i = files.Length - 2; i >= 0; i--)
                    {
                        var file = files[i];
                        var chunk = Chunk.FromCompletedFile(file, this, _config);
                        if (_config.EnableCache && cachedChunkCount < _config.PreCacheChunkCount)
                        {
                            if (chunk.TryCacheInMemory(false))
                            {
                                cachedChunkCount++;
                            }
                        }
                        AddChunk(chunk);
                    }
                    var lastFile = files[files.Length - 1];
                    AddChunk(Chunk.FromOngoingFile(lastFile, this, _config, readRecordFunc));
                }

                if (_config.EnableCache)
                {
                    _scheduleService.StartTask("UncacheChunks", () => UncacheChunks(), 1000, 1000);
                }
            }
        }
        public int GetChunkCount()
        {
            return _chunks.Count;
        }
        public IList<Chunk> GetAllChunks()
        {
            return _chunks.Values.ToList();
        }
        public Chunk AddNewChunk()
        {
            lock (_lockObj)
            {
                var chunkNumber = _nextChunkNumber;
                var chunkFileName = _config.FileNamingStrategy.GetFileNameFor(_chunkPath, chunkNumber);
                var chunk = Chunk.CreateNew(chunkFileName, chunkNumber, this, _config);

                AddChunk(chunk);

                return chunk;
            }
        }
        public Chunk GetFirstChunk()
        {
            lock (_lockObj)
            {
                if (_chunks.Count == 0)
                {
                    AddNewChunk();
                }
                var minChunkNum = _chunks.Keys.Min();
                return _chunks[minChunkNum];
            }
        }
        public Chunk GetLastChunk()
        {
            lock (_lockObj)
            {
                if (_chunks.Count == 0)
                {
                    AddNewChunk();
                }
                return _chunks[_nextChunkNumber - 1];
            }
        }
        public int GetChunkNum(long dataPosition)
        {
            return (int)(dataPosition / _config.GetChunkDataSize());
        }
        public Chunk GetChunkFor(long dataPosition)
        {
            var chunkNum = (int)(dataPosition / _config.GetChunkDataSize());
            return GetChunk(chunkNum);
        }
        public Chunk GetChunk(int chunkNum)
        {
            if (_chunks.ContainsKey(chunkNum))
            {
                return _chunks[chunkNum];
            }
            return null;
        }
        public bool RemoveChunk(Chunk chunk)
        {
            lock (_lockObj)
            {
                if (_chunks.Remove(chunk.ChunkHeader.ChunkNumber))
                {
                    try
                    {
                        chunk.Destroy();
                    }
                    catch (Exception ex)
                    {
                        _logger.Error(string.Format("Destroy chunk {0} has exception.", chunk), ex);
                    }
                    return true;
                }
                return false;
            }
        }
        public void TryCacheNextChunk(Chunk currentChunk)
        {
            if (!_config.EnableCache) return;

            if (Interlocked.CompareExchange(ref _isCachingNextChunk, 1, 0) == 0)
            {
                try
                {
                    var nextChunkNumber = currentChunk.ChunkHeader.ChunkNumber + 1;
                    var nextChunk = GetChunk(nextChunkNumber);
                    if (nextChunk != null && !nextChunk.IsMemoryChunk && nextChunk.IsCompleted && !nextChunk.HasCachedChunk)
                    {
                        Task.Factory.StartNew(() => nextChunk.TryCacheInMemory(false));
                    }
                }
                finally
                {
                    Interlocked.Exchange(ref _isCachingNextChunk, 0);
                }
            }
        }

        public void Dispose()
        {
            Close();
        }
        public void Close()
        {
            lock (_lockObj)
            {
                _scheduleService.StopTask("UncacheChunks");

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

        private void AddChunk(Chunk chunk)
        {
            _chunks.Add(chunk.ChunkHeader.ChunkNumber, chunk);
            _nextChunkNumber = chunk.ChunkHeader.ChunkNumber + 1;
        }
        private int UncacheChunks(int maxUncacheCount = 10)
        {
            var uncachedCount = 0;

            if (Interlocked.CompareExchange(ref _uncachingChunks, 1, 0) == 0)
            {
                try
                {
                    var usedMemoryPercent = ChunkUtil.GetUsedMemoryPercent();
                    if (usedMemoryPercent <= (ulong)_config.ChunkCacheMinPercent)
                    {
                        return 0;
                    }

                    if (_logger.IsDebugEnabled)
                    {
                        _logger.DebugFormat("Current memory usage {0}% exceed the chunkCacheMinPercent {1}%, try to uncache chunks.", usedMemoryPercent, _config.ChunkCacheMinPercent);
                    }

                    var chunks = _chunks.Values.Where(x => x.IsCompleted && !x.IsMemoryChunk && x.HasCachedChunk).OrderBy(x => x.LastActiveTime).ToList();

                    foreach (var chunk in chunks)
                    {
                        if ((DateTime.Now - chunk.LastActiveTime).TotalSeconds >= _config.ChunkInactiveTimeMaxSeconds)
                        {
                            if (chunk.UnCacheFromMemory())
                            {
                                Thread.Sleep(100); //即便有内存释放了，由于通过API读取到的内存使用数可能不会立即更新，所以等待一定时间后检查内存是否足够
                                uncachedCount++;
                                if (uncachedCount >= maxUncacheCount || ChunkUtil.GetUsedMemoryPercent() <= (ulong)_config.ChunkCacheMinPercent)
                                {
                                    break;
                                }
                            }
                        }
                    }

                    if (_logger.IsDebugEnabled)
                    {
                        if (uncachedCount > 0)
                        {
                            _logger.DebugFormat("Uncached {0} chunks, current memory usage: {1}%", uncachedCount, ChunkUtil.GetUsedMemoryPercent());
                        }
                        else
                        {
                            _logger.Debug("No chunks uncached.");
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.Error("Uncaching chunks has exception.", ex);
                }
                finally
                {
                    Interlocked.Exchange(ref _uncachingChunks, 0);
                }
            }

            return uncachedCount;
        }
    }
}
