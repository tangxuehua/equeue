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
    public unsafe partial class TFChunk : IDisposable
    {
        #region Variables and Properties

        public const byte CurrentChunkVersion = 1;
        public const int WriteBufferSize = 8192;
        public const int ReadBufferSize = 8192;

        private static readonly ILogger _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(typeof(TFChunk));
        private static readonly ILogRecordParserProvider _logRecordParserProvider = ObjectContainer.Resolve<ILogRecordParserProvider>();

        private readonly bool _inMem;
        private readonly string _filename;
        private long _fileSize;
        private volatile bool _isReadOnly;
        private ChunkHeader _chunkHeader;
        private ChunkFooter _chunkFooter;

        private readonly int _maxReaderCount;
        private readonly ConcurrentQueue<ReaderWorkItem> _fileReaderWorkItemQueue = new ConcurrentQueue<ReaderWorkItem>();
        private readonly ConcurrentQueue<ReaderWorkItem> _memStreams = new ConcurrentQueue<ReaderWorkItem>();
        private int _internalStreamsCount;
        private int _fileStreamCount;
        private int _memStreamCount;

        private WriterWorkItem _writerWorkItem;
        private volatile int _dataPosition;

        private volatile int _isCached;
        private volatile IntPtr _cachedData;
        private int _cachedLength;

        private readonly ManualResetEventSlim _destroyEvent = new ManualResetEventSlim(false);
        private volatile bool _selfdestructin54321;
        private volatile bool _deleteFile;

        private IChunkReadSide _readSide;

        public bool IsReadOnly { get { return _isReadOnly; } }
        public bool IsCached { get { return _isCached != 0; } }

        // the current data position
        public int DataPosition { get { return _dataPosition; } }

        public string FileName { get { return _filename; } }
        public long FileSize { get { return _fileSize; } }

        public ChunkHeader ChunkHeader { get { return _chunkHeader; } }
        public ChunkFooter ChunkFooter { get { return _chunkFooter; } }
        public int RawWriterPosition
        {
            get
            {
                var writerWorkItem = _writerWorkItem;
                if (writerWorkItem == null)
                {
                    throw new InvalidOperationException(string.Format("TFChunk {0} is not in write mode.", _filename));
                }
                return (int)writerWorkItem.WorkingStream.Position;
            }
        }

        #endregion

        #region Constructors

        private TFChunk(string filename, int initialReaderCount, int maxReaderCount, bool inMem)
        {
            Ensure.NotNullOrEmpty(filename, "filename");
            Ensure.Positive(initialReaderCount, "initialReaderCount");
            Ensure.Positive(maxReaderCount, "maxReaderCount");
            if (initialReaderCount > maxReaderCount)
            {
                throw new ArgumentOutOfRangeException("initialReaderCount", "initialReaderCount is greater than maxReaderCount.");
            }

            _filename = filename;
            _internalStreamsCount = initialReaderCount;
            _maxReaderCount = maxReaderCount;
            _inMem = inMem;
        }
        ~TFChunk()
        {
            FreeCachedData();
        }

        #endregion

        #region Factory Methods

        public static TFChunk FromCompletedFile(string filename)
        {
            var chunk = new TFChunk(filename, Consts.TFChunkInitialReaderCount, Consts.TFChunkMaxReaderCount, false);
            try
            {
                chunk.InitCompleted();
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("Chunk {0} init from completed file failed.", chunk), ex);
                chunk.Dispose();
                throw;
            }
            return chunk;
        }
        public static TFChunk FromOngoingFile(string filename, int dataPosition, bool checkSize)
        {
            var chunk = new TFChunk(filename, Consts.TFChunkInitialReaderCount, Consts.TFChunkMaxReaderCount, false);
            try
            {
                chunk.InitOngoing(dataPosition, checkSize);
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("Chunk {0} init from ongoing file failed.", chunk), ex);
                chunk.Dispose();
                throw;
            }
            return chunk;
        }
        public static TFChunk CreateNew(string filename, int chunkSize, int chunkStartNumber, int chunkEndNumber, bool inMem = false)
        {
            var header = new ChunkHeader(CurrentChunkVersion, chunkSize, chunkStartNumber, chunkEndNumber, Guid.NewGuid());
            return CreateWithHeader(filename, header, chunkSize + ChunkHeader.Size + ChunkFooter.Size, inMem);
        }
        public static TFChunk CreateWithHeader(string filename, ChunkHeader header, int fileSize, bool inMem)
        {
            var chunk = new TFChunk(filename, Consts.TFChunkInitialReaderCount, Consts.TFChunkMaxReaderCount, inMem);
            try
            {
                chunk.InitNew(header, fileSize);
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("Chunk {0} create failed.", chunk), ex);
                chunk.Dispose();
                throw;
            }
            return chunk;
        }

        #endregion

        #region Init Methods

        private void InitCompleted()
        {
            var fileInfo = new FileInfo(_filename);
            if (!fileInfo.Exists)
            {
                throw new CorruptDatabaseException(new ChunkNotFoundException(_filename));
            }

            _fileSize = (int)fileInfo.Length;
            _isReadOnly = true;

            SetAttributes();
            CreateFileReaderWorkItems();

            var reader = GetReaderWorkItem();
            try
            {
                _chunkHeader = ReadHeader(reader.Stream);
                if (_chunkHeader.Version != CurrentChunkVersion)
                {
                    throw new CorruptDatabaseException(new WrongFileVersionException(_filename, _chunkHeader.Version, CurrentChunkVersion));
                }

                _chunkFooter = ReadFooter(reader.Stream);
                if (!_chunkFooter.IsCompleted)
                {
                    throw new CorruptDatabaseException(new BadChunkInDatabaseException(string.Format("Chunk file '{0}' should be completed, but is not.", _filename)));
                }

                _dataPosition = _chunkFooter.DataPosition;

                var expectedFileSize = _chunkFooter.DataPosition + ChunkHeader.Size + ChunkFooter.Size;
                if (reader.Stream.Length != expectedFileSize)
                {
                    throw new CorruptDatabaseException(new BadChunkInDatabaseException(
                        string.Format("Chunk file '{0}' should have file size {1} bytes, but instead has {2} bytes length.",
                                      _filename,
                                      expectedFileSize,
                                      reader.Stream.Length)));
                }
            }
            finally
            {
                ReturnReaderWorkItem(reader);
            }

            _readSide = new TFChunkReadSideUnscavenged(this);
        }
        private void InitNew(ChunkHeader chunkHeader, int fileSize)
        {
            Ensure.NotNull(chunkHeader, "chunkHeader");
            Ensure.Positive(fileSize, "fileSize");

            _isReadOnly = false;
            _chunkHeader = chunkHeader;
            _fileSize = fileSize;
            _dataPosition = 0;

            if (_inMem)
            {
                CreateInMemChunk(chunkHeader, fileSize);
            }
            else
            {
                CreateFileChunk(chunkHeader, fileSize);
            }

            _readSide = new TFChunkReadSideUnscavenged(this);
        }
        private void InitOngoing(int dataPosition, bool checkSize)
        {
            Ensure.Nonnegative(dataPosition, "dataPosition");

            var fileInfo = new FileInfo(_filename);
            if (!fileInfo.Exists)
            {
                throw new CorruptDatabaseException(new ChunkNotFoundException(_filename));
            }

            _fileSize = (int)fileInfo.Length;
            _isReadOnly = false;
            _dataPosition = dataPosition;

            SetAttributes();

            var fileStream = new FileStream(_filename, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, WriteBufferSize, FileOptions.SequentialScan);
            try
            {
                _chunkHeader = ReadHeader(fileStream);
            }
            catch
            {
                fileStream.Dispose();
                throw;
            }

            if (_chunkHeader.Version != CurrentChunkVersion)
            {
                throw new CorruptDatabaseException(new WrongFileVersionException(_filename, _chunkHeader.Version, CurrentChunkVersion));
            }

            fileStream.Position = GetStreamPosition(dataPosition);
            _writerWorkItem = new WriterWorkItem(fileStream, null);

            CreateFileReaderWorkItems();

            if (checkSize)
            {
                var expectedFileSize = _chunkHeader.ChunkDataSize + ChunkHeader.Size + ChunkFooter.Size;
                if (_writerWorkItem.WorkingStream.Length != expectedFileSize)
                {
                    throw new CorruptDatabaseException(new BadChunkInDatabaseException(
                        string.Format("Chunk file '{0}' should have file size {1} bytes, but instead has {2} bytes length.",
                                      _filename,
                                      expectedFileSize,
                                      _writerWorkItem.WorkingStream.Length)));
                }
            }

            _readSide = new TFChunkReadSideUnscavenged(this);
        }

        #endregion

        #region Cache & UnCache Methods

        // WARNING CacheInMemory/UncacheFromMemory should not be called simultaneously !!!
        public void CacheInMemory()
        {
            #pragma warning disable 0420
            if (_inMem || Interlocked.CompareExchange(ref _isCached, 1, 0) != 0)
            {
                return;
            }
            #pragma warning restore 0420

            // we won the right to cache
            var sw = Stopwatch.StartNew();
            try
            {
                BuildCacheArray();
            }
            catch (OutOfMemoryException)
            {
                _logger.WarnFormat("CACHING FAILED due to OutOfMemory exception in TFChunk {0}.", this);
                _isCached = 0;
                return;
            }
            catch (FileBeingDeletedException)
            {
                _logger.WarnFormat("CACHING FAILED due to FileBeingDeleted exception (TFChunk is being disposed) in TFChunk {0}.", this);
                _isCached = 0;
                return;
            }

            Interlocked.Add(ref _memStreamCount, _maxReaderCount);
            if (_selfdestructin54321)
            {
                if (Interlocked.Add(ref _memStreamCount, -_maxReaderCount) == 0)
                {
                    FreeCachedData();
                }
                _logger.WarnFormat("CACHING ABORTED for TFChunk {0} as TFChunk was probably marked for deletion.", this);
                return;
            }

            var writerWorkItem = _writerWorkItem;
            if (writerWorkItem != null)
            {
                var memStream = new UnmanagedMemoryStream((byte*)_cachedData, _cachedLength, _cachedLength, FileAccess.ReadWrite);
                memStream.Position = writerWorkItem.WorkingStream.Position;
                writerWorkItem.SetMemStream(memStream);
            }

            for (int i = 0; i < _maxReaderCount; i++)
            {
                var memStream = new UnmanagedMemoryStream((byte*)_cachedData, _cachedLength);
                var reader = new BinaryReader(memStream);
                _memStreams.Enqueue(new ReaderWorkItem(memStream, reader, isMemory: true));
            }

            _logger.InfoFormat("CACHED TFChunk {0} in {1}ms.", this, sw.ElapsedMilliseconds);

            if (_selfdestructin54321)
            {
                TryDestructMemStreams();
            }
        }
        //WARNING CacheInMemory/UncacheFromMemory should not be called simultaneously !!!
        public void UnCacheFromMemory()
        {
            if (_inMem)
            {
                return;
            }

#pragma warning disable 0420
            if (Interlocked.CompareExchange(ref _isCached, 0, 1) == 1)
#pragma warning restore 0420

            {
                // we won the right to un-cache and chunk was cached
                // NOTE: calling simultaneously cache and uncache is very dangerous
                // NOTE: though multiple simultaneous calls to either Cache or Uncache is ok

                TryDestructMemStreams();

                _logger.InfoFormat("UNCACHED TFChunk {0}.", this);
            }
        }

        private void BuildCacheArray()
        {
            var workItem = GetReaderWorkItem();
            try
            {
                if (workItem.IsMemory)
                {
                    throw new InvalidOperationException("When trying to build cache, reader workitem is already in-memory reader.");
                }

                var dataSize = _isReadOnly ? (int)_dataPosition : _chunkHeader.ChunkDataSize;
                _cachedLength = ChunkHeader.Size + dataSize + ChunkFooter.Size;
                var cachedData = Marshal.AllocHGlobal(_cachedLength);
                try
                {
                    using (var unmanagedStream = new UnmanagedMemoryStream((byte*)cachedData, _cachedLength, _cachedLength, FileAccess.ReadWrite))
                    {
                        workItem.Stream.Seek(0, SeekOrigin.Begin);
                        var buffer = new byte[65536];
                        // in ongoing chunk there is no need to read everything, it's enough to read just actual data written
                        long toRead = _isReadOnly ? _cachedLength : ChunkHeader.Size + _dataPosition;
                        while (toRead > 0)
                        {
                            int read = workItem.Stream.Read(buffer, 0, Math.Min((int)toRead, buffer.Length));
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
            }
            finally
            {
                ReturnReaderWorkItem(workItem);
            }
        }

        #endregion

        #region Read Methods

        public bool ExistsAt(long dataPosition)
        {
            return _readSide.ExistsAt(dataPosition);
        }
        public RecordReadResult TryReadAt(long dataPosition)
        {
            return _readSide.TryReadAt(dataPosition);
        }
        public RecordReadResult TryReadFirst()
        {
            return _readSide.TryReadFirst();
        }
        public RecordReadResult TryReadClosestForward(long dataPosition)
        {
            return _readSide.TryReadClosestForward(dataPosition);
        }
        public RecordReadResult TryReadLast()
        {
            return _readSide.TryReadLast();
        }
        public RecordReadResult TryReadClosestBackward(long dataPosition)
        {
            return _readSide.TryReadClosestBackward(dataPosition);
        }

        #endregion

        #region Append Methods

        public RecordWriteResult TryAppend(ILogRecord record)
        {
            if (_isReadOnly)
            {
                throw new InvalidOperationException(string.Format("Cannot write to a read-only chunk: {0}", this));
            }

            var writerWorkItem = _writerWorkItem;
            var bufferStream = writerWorkItem.BufferStream;
            var bufferStreamWriter = writerWorkItem.BufferWriter;

            bufferStream.SetLength(4);
            bufferStream.Position = 4;
            bufferStreamWriter.Write(record.Type);
            record.WriteTo(bufferStreamWriter);
            var recordLength = (int)bufferStream.Length - 4;
            bufferStreamWriter.Write(recordLength); // write record length suffix
            bufferStream.Position = 0;
            bufferStreamWriter.Write(recordLength); // write record length prefix

            if (writerWorkItem.WorkingStream.Position + recordLength + 2 * sizeof(int) > ChunkHeader.Size + _chunkHeader.ChunkDataSize)
            {
                return RecordWriteResult.Failed(GetDataPosition(writerWorkItem));
            }

            var oldDataPosition = GetDataPosition(writerWorkItem);
            writerWorkItem.AppendData(bufferStream.GetBuffer(), 0, (int)bufferStream.Length);
            _dataPosition = GetDataPosition(writerWorkItem);

            return RecordWriteResult.Successful(oldDataPosition, _dataPosition);
        }

        #endregion

        #region Complete Methods

        public void Flush()
        {
            if (_inMem)
            {
                return;
            }

            if (_isReadOnly)
            {
                throw new InvalidOperationException(string.Format("Cannot flush a read-only TFChunk: {0}", this));
            }

            _writerWorkItem.FlushToDisk();
        }
        public void Complete()
        {
            if (_isReadOnly)
            {
                throw new InvalidOperationException(string.Format("Cannot complete a read-only TFChunk: {0}", this));
            }

            _chunkFooter = WriteFooter();
            Flush();
            _isReadOnly = true;

            _writerWorkItem.Dispose();
            _writerWorkItem = null;
            SetAttributes();
        }

        #endregion

        #region Clean Methods

        public void Dispose()
        {
            _selfdestructin54321 = true;
            TryDestructFileStreams();
            TryDestructMemStreams();
        }
        public void MarkForDeletion()
        {
            _selfdestructin54321 = true;
            _deleteFile = true;
            TryDestructFileStreams();
            TryDestructMemStreams();
        }
        public void WaitForDestroy(int timeoutMs)
        {
            if (!_destroyEvent.Wait(timeoutMs))
            {
                throw new TimeoutException();
            }
        }

        private void TryDestructFileStreams()
        {
            int fileStreamCount = int.MaxValue;

            ReaderWorkItem workItem;
            while (_fileReaderWorkItemQueue.TryDequeue(out workItem))
            {
                workItem.Stream.Dispose();
                fileStreamCount = Interlocked.Decrement(ref _fileStreamCount);
            }

            if (fileStreamCount < 0)
            {
                throw new Exception("Somehow we managed to decrease count of file streams below zero.");
            }
            if (fileStreamCount == 0) // we are the last who should "turn the light off" for file streams
            {
                CleanFileStreamDestruction();
            }
        }
        private bool TryDestructMemStreams()
        {
            var writerWorkItem = _writerWorkItem;
            if (writerWorkItem != null)
            {
                writerWorkItem.DisposeMemStream();
            }

            int memStreamCount = int.MaxValue;

            ReaderWorkItem workItem;
            while (_memStreams.TryDequeue(out workItem))
            {
                memStreamCount = Interlocked.Decrement(ref _memStreamCount);
            }
            if (memStreamCount < 0)
            {
                throw new Exception("Somehow we managed to decrease count of memory streams below zero.");
            }
            if (memStreamCount == 0) // we are the last who should "turn the light off" for memory streams
            {
                FreeCachedData();
                return true;
            }
            return false;
        }
        private void CleanFileStreamDestruction()
        {
            if (_writerWorkItem != null)
            {
                _writerWorkItem.Dispose();
            }

            if (!_inMem)
            {
                Helper.EatException(() => File.SetAttributes(_filename, FileAttributes.Normal));

                if (_deleteFile)
                {
                    _logger.InfoFormat("File {0} has been marked for delete and will be deleted in TryDestructFileStreams.", Path.GetFileName(_filename));
                    Helper.EatException(() => File.Delete(_filename));
                }
            }

            _destroyEvent.Set();
        }
        private void FreeCachedData()
        {
            #pragma warning disable 0420
            var cachedData = Interlocked.Exchange(ref _cachedData, IntPtr.Zero);
            #pragma warning restore 0420
            if (cachedData != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(cachedData);
            }
        }

        #endregion

        #region Helper Methods

        private void CreateFileReaderWorkItems()
        {
            Interlocked.Add(ref _fileStreamCount, _internalStreamsCount);
            for (int i = 0; i < _internalStreamsCount; i++)
            {
                _fileReaderWorkItemQueue.Enqueue(CreateFileReaderWorkItem());
            }
        }
        private ReaderWorkItem CreateFileReaderWorkItem()
        {
            var fileStream = new FileStream(_filename, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, ReadBufferSize, FileOptions.RandomAccess);
            var reader = new BinaryReader(fileStream);
            return new ReaderWorkItem(fileStream, reader, false);
        }
        private ReaderWorkItem GetReaderWorkItem()
        {
            if (_selfdestructin54321)
            {
                throw new FileBeingDeletedException();
            }

            ReaderWorkItem item;
            if (_memStreams.TryDequeue(out item))
            {
                return item;
            }

            if (_inMem)
            {
                throw new Exception("Not enough memory streams during in-mem TFChunk mode.");
            }

            if (_fileReaderWorkItemQueue.TryDequeue(out item))
            {
                return item;
            }

            if (_selfdestructin54321)
            {
                throw new FileBeingDeletedException();
            }

            var internalStreamCount = Interlocked.Increment(ref _internalStreamsCount);
            if (internalStreamCount > _maxReaderCount)
            {
                throw new Exception("Unable to acquire reader work item. Max internal streams limit reached.");
            }
            Interlocked.Increment(ref _fileStreamCount);

            if (_selfdestructin54321)
            {
                if (Interlocked.Decrement(ref _fileStreamCount) == 0)
                {
                    CleanFileStreamDestruction(); // now we should "turn light off"
                }
                throw new FileBeingDeletedException();
            }

            // if we get here, then we reserved TFChunk for sure so no one should dispose of chunk file until client returns the reader
            return CreateFileReaderWorkItem();
        }
        private void ReturnReaderWorkItem(ReaderWorkItem item)
        {
            if (item.IsMemory)
            {
                _memStreams.Enqueue(item);
                if (_isCached == 0 || _selfdestructin54321)
                {
                    TryDestructMemStreams();
                }
            }
            else
            {
                _fileReaderWorkItemQueue.Enqueue(item);
                if (_selfdestructin54321)
                {
                    TryDestructFileStreams();
                }
            }
        }

        private void CreateInMemChunk(ChunkHeader chunkHeader, int fileSize)
        {
            // ALLOCATE MEM
#pragma warning disable 0420
            Interlocked.Exchange(ref _isCached, 1);
#pragma warning restore 0420

            _cachedLength = fileSize;
            _cachedData = Marshal.AllocHGlobal(_cachedLength);

            // WRITER STREAM
            var memStream = new UnmanagedMemoryStream((byte*)_cachedData, _cachedLength, _cachedLength, FileAccess.ReadWrite);
            WriteHeader(memStream, chunkHeader);
            memStream.Position = ChunkHeader.Size;
            _writerWorkItem = new WriterWorkItem(null, memStream);

            // READER STREAMS
            Interlocked.Add(ref _memStreamCount, _maxReaderCount);
            for (int i = 0; i < _maxReaderCount; i++)
            {
                var stream = new UnmanagedMemoryStream((byte*)_cachedData, _cachedLength);
                var reader = new BinaryReader(stream);
                _memStreams.Enqueue(new ReaderWorkItem(stream, reader, isMemory: true));
            }
        }
        private void CreateFileChunk(ChunkHeader chunkHeader, int fileSize)
        {
            // create temp file first and set desired length
            // if there is not enough disk space or something else prevents file to be resized as desired
            // we'll end up with empty temp file, which won't trigger false error on next DB verification
            var tempFilename = string.Format("{0}.{1}.tmp", _filename, Guid.NewGuid());
            var tempFileStream = new FileStream(tempFilename, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read, WriteBufferSize, FileOptions.SequentialScan);
            tempFileStream.SetLength(fileSize);

            // we need to write header into temp file before moving it into correct chunk place, so in case of crash
            // we don't end up with seemingly valid chunk file with no header at all...
            WriteHeader(tempFileStream, chunkHeader);

            tempFileStream.FlushToDisk();
            tempFileStream.Close();

            File.Move(tempFilename, _filename);

            var fileStream = new FileStream(_filename, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, WriteBufferSize, FileOptions.SequentialScan);
            fileStream.Position = ChunkHeader.Size;
            _writerWorkItem = new WriterWorkItem(fileStream, null);

            Flush(); // persist file move result

            SetAttributes();
            CreateFileReaderWorkItems();
        }

        private void SetAttributes()
        {
            if (_inMem)
            {
                return;
            }

            Helper.EatException(() =>
            {
                if (_isReadOnly)
                {
                    File.SetAttributes(_filename, FileAttributes.ReadOnly | FileAttributes.NotContentIndexed);
                }
                else
                {
                    File.SetAttributes(_filename, FileAttributes.NotContentIndexed);
                }
            });
        }

        private void WriteHeader(Stream stream, ChunkHeader chunkHeader)
        {
            stream.Write(chunkHeader.AsByteArray(), 0, ChunkHeader.Size);
        }
        private ChunkFooter WriteFooter()
        {
            var workItem = _writerWorkItem;
            var footer = new ChunkFooter(true, _dataPosition);

            workItem.AppendData(footer.AsByteArray(), 0, ChunkFooter.Size);

            Flush(); // trying to prevent bug with resized file, but no data in it

            var fileSize = ChunkHeader.Size + _dataPosition + ChunkFooter.Size;
            if (workItem.WorkingStream.Length != fileSize)
            {
                workItem.ResizeStream(fileSize);
                _fileSize = fileSize;
            }

            return footer;
        }
        private ChunkHeader ReadHeader(Stream stream)
        {
            if (stream.Length < ChunkHeader.Size)
            {
                throw new Exception(string.Format("Chunk file '{0}' is too short to even read ChunkHeader, its size is {1} bytes.", _filename, stream.Length));
            }
            stream.Seek(0, SeekOrigin.Begin);
            return ChunkHeader.FromStream(stream);
        }
        private ChunkFooter ReadFooter(Stream stream)
        {
            if (stream.Length < ChunkFooter.Size)
            {
                throw new Exception(string.Format("Chunk file '{0}' is too short to even read ChunkFooter, its size is {1} bytes.", _filename, stream.Length));
            }
            stream.Seek(-ChunkFooter.Size, SeekOrigin.End);
            return ChunkFooter.FromStream(stream);
        }

        private static long GetStreamPosition(long dataPosition)
        {
            return ChunkHeader.Size + dataPosition;
        }
        private static int GetDataPosition(WriterWorkItem workItem)
        {
            return (int)workItem.WorkingStream.Position - ChunkHeader.Size;
        }

        #endregion

        public override string ToString()
        {
            return string.Format("#{0}-{1} ({2})", _chunkHeader.ChunkStartNumber, _chunkHeader.ChunkEndNumber, Path.GetFileName(_filename));
        }
    }
}
