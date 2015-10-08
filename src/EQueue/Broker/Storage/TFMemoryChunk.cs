using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Utilities;

namespace EQueue.Broker.Storage
{
    public unsafe class TFMemoryChunk : IDisposable
    {
        #region Private Variables

        private static readonly ILogger _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(typeof(TFMemoryChunk));

        private readonly TFChunk _fileChunk;
        private readonly ConcurrentQueue<ReaderWorkItem> _readerWorkItemQueue = new ConcurrentQueue<ReaderWorkItem>();
        private readonly object _lockObj = new object();

        private IntPtr _cachedData;
        private int _cachedLength;
        private bool _isInitialized;

        #endregion

        #region Public Properties

        public TFChunk FileChunk { get { return _fileChunk; } }
        public bool IsInitialized { get { return _isInitialized; } }

        #endregion

        #region Constructors

        public TFMemoryChunk(TFChunk fileChunk)
        {
            Ensure.NotNull(fileChunk, "fileChunk");

            if (!fileChunk.IsCompleted)
            {
                throw new ArgumentException(string.Format("File chunk {0} must be completed.", fileChunk));
            }

            _fileChunk = fileChunk;
        }

        #endregion

        public void Initialize()
        {
            lock (_lockObj)
            {
                if (!_isInitialized)
                {
                    LoadFileChunkToMemory();
                    InitializeReaderWorkItems();
                    _isInitialized = true;
                }
            }
        }
        public T TryReadAt<T>(long dataPosition, Func<int, BinaryReader, T> readRecordFunc) where T : ILogRecord
        {
            var readerWorkItem = GetReaderWorkItem();
            try
            {
                var currentDataPosition = _fileChunk.DataPosition;
                if (dataPosition >= _fileChunk.DataPosition)
                {
                    throw new InvalidReadException(
                        string.Format("Cannot read record after the max data position, data position: {0}, max data position: {1}, chunk: {2}.",
                                      dataPosition, currentDataPosition, this));
                }

                return _fileChunk.IsFixedDataSize() ?
                    TryReadFixedSizeForwardInternal(readerWorkItem, dataPosition, readRecordFunc) :
                    TryReadForwardInternal(readerWorkItem, dataPosition, readRecordFunc);
            }
            finally
            {
                ReturnReaderWorkItem(readerWorkItem);
            }
        }

        #region Clean Methods

        public void Dispose()
        {
            Close();
        }
        public void Close()
        {
            CloseAllReaderWorkItems();
        }

        #endregion

        #region Helper Methods

        private void LoadFileChunkToMemory()
        {
            var watch = Stopwatch.StartNew();

            using (var fileStream = new FileStream(_fileChunk.FileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 8192, FileOptions.RandomAccess))
            {
                var cachedLength = (int)fileStream.Length;
                var cachedData = Marshal.AllocHGlobal(cachedLength);

                try
                {
                    using (var unmanagedStream = new UnmanagedMemoryStream((byte*)cachedData, cachedLength, cachedLength, FileAccess.ReadWrite))
                    {
                        fileStream.Seek(0, SeekOrigin.Begin);
                        var buffer = new byte[65536];
                        int toRead = cachedLength;
                        while (toRead > 0)
                        {
                            int read = fileStream.Read(buffer, 0, Math.Min(toRead, buffer.Length));
                            if (read == 0)
                            {
                                break;
                            }
                            toRead -= read;
                            unmanagedStream.Write(buffer, 0, read);
                        }
                    }
                }
                catch
                {
                    Marshal.FreeHGlobal(cachedData);
                    throw;
                }

                _cachedData = cachedData;
                _cachedLength = cachedLength;
            }

            _logger.InfoFormat("Load file chunk {0} to memory success, timeSpent: {1}ms", _fileChunk, watch.ElapsedMilliseconds);
        }
        private T TryReadForwardInternal<T>(ReaderWorkItem readerWorkItem, long dataPosition, Func<int, BinaryReader, T> readRecordFunc) where T : ILogRecord
        {
            var currentDataPosition = _fileChunk.DataPosition;

            if (dataPosition + 2 * sizeof(int) > currentDataPosition)
            {
                throw new InvalidReadException(
                    string.Format("No enough space even for length prefix and suffix, data position: {0}, max data position: {1}, chunk {2}.",
                                  dataPosition, currentDataPosition, this));
            }

            readerWorkItem.Stream.Position = GetStreamPosition(dataPosition);

            var length = readerWorkItem.Reader.ReadInt32();
            if (length <= 0)
            {
                throw new InvalidReadException(
                    string.Format("Log record at data position {0} has non-positive length: {1} in chunk {2}",
                                  dataPosition, length, this));
            }
            if (length > _fileChunk.Config.MaxLogRecordSize)
            {
                throw new InvalidReadException(
                    string.Format("Log record at data position {0} has too large length: {1} bytes, while limit is {2} bytes, in chunk {3}",
                                  dataPosition, length, _fileChunk.Config.MaxLogRecordSize, this));
            }
            if (dataPosition + length + 2 * sizeof(int) > currentDataPosition)
            {
                throw new InvalidReadException(
                    string.Format("There is not enough space to read full record (length prefix: {0}), data position: {1}, max data position: {2}, chunk {3}.",
                                  length, dataPosition, currentDataPosition, this));
            }

            var record = readRecordFunc(length, readerWorkItem.Reader);
            if (record == null)
            {
                throw new InvalidReadException(
                    string.Format("Cannot read a record from data position {0}. Something is seriously wrong in chunk {1}.",
                                  dataPosition, this));
            }

            int suffixLength = readerWorkItem.Reader.ReadInt32();
            if (suffixLength != length)
            {
                throw new InvalidReadException(
                    string.Format("Prefix/suffix length inconsistency: prefix length({0}) != suffix length ({1}), data position: {2}. Something is seriously wrong in chunk {3}.",
                                  length, suffixLength, dataPosition, this));
            }

            return record;
        }
        private T TryReadFixedSizeForwardInternal<T>(ReaderWorkItem readerWorkItem, long dataPosition, Func<int, BinaryReader, T> readRecordFunc) where T : ILogRecord
        {
            var currentDataPosition = _fileChunk.DataPosition;

            if (dataPosition + _fileChunk.Config.ChunkDataUnitSize > currentDataPosition)
            {
                throw new InvalidReadException(
                    string.Format("No enough space for fixed data record, data position: {0}, max data position: {1}, chunk {2}.",
                                  dataPosition, currentDataPosition, this));
            }

            var startStreamPosition = GetStreamPosition(dataPosition);
            readerWorkItem.Stream.Position = startStreamPosition;

            var record = readRecordFunc(_fileChunk.Config.ChunkDataUnitSize, readerWorkItem.Reader);
            if (record == null)
            {
                throw new InvalidReadException(
                        string.Format("Read fixed record from data position: {0} failed, max data position: {1}. Something is seriously wrong in chunk {2}.",
                                      dataPosition, currentDataPosition, this));
            }

            var recordLength = readerWorkItem.Stream.Position - startStreamPosition;
            if (recordLength != _fileChunk.Config.ChunkDataUnitSize)
            {
                throw new InvalidReadException(
                        string.Format("Invalid fixed record length, expected length {0}, but was {1}, dataPosition: {2}. Something is seriously wrong in chunk {3}.",
                                      _fileChunk.Config.ChunkDataUnitSize, recordLength, dataPosition, this));
            }

            return record;
        }
        private void InitializeReaderWorkItems()
        {
            for (var i = 0; i < _fileChunk.Config.ChunkReaderCount; i++)
            {
                _readerWorkItemQueue.Enqueue(CreateReaderWorkItem());
            }
        }
        private void CloseAllReaderWorkItems()
        {
            var watch = Stopwatch.StartNew();
            var closedCount = 0;

            while (closedCount < _fileChunk.Config.ChunkReaderCount)
            {
                ReaderWorkItem readerWorkItem;
                while (_readerWorkItemQueue.TryDequeue(out readerWorkItem))
                {
                    readerWorkItem.Reader.Close();
                    closedCount++;
                }

                if (closedCount >= _fileChunk.Config.ChunkReaderCount)
                {
                    break;
                }

                Thread.Sleep(1000);

                if (watch.ElapsedMilliseconds > 30 * 1000)
                {
                    _logger.ErrorFormat("Close chunk reader work items timeout, expect close count: {0}, real close count: {1}", _fileChunk.Config.ChunkReaderCount, closedCount);
                    break;
                }
            }
        }
        private ReaderWorkItem CreateReaderWorkItem()
        {
            var memoryStream = new UnmanagedMemoryStream((byte*)_cachedData, _cachedLength);
            var reader = new BinaryReader(memoryStream);
            return new ReaderWorkItem(memoryStream, reader);
        }
        private ReaderWorkItem GetReaderWorkItem()
        {
            ReaderWorkItem readerWorkItem;
            while (!_readerWorkItemQueue.TryDequeue(out readerWorkItem))
            {
                Thread.Sleep(1);
            }
            return readerWorkItem;
        }
        private void ReturnReaderWorkItem(ReaderWorkItem readerWorkItem)
        {
            _readerWorkItemQueue.Enqueue(readerWorkItem);
        }

        private static long GetStreamPosition(long dataPosition)
        {
            return ChunkHeader.Size + dataPosition;
        }

        #endregion
    }
}
