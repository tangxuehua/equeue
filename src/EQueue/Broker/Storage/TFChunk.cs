using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Utilities;

namespace EQueue.Broker.Storage
{
    public unsafe partial class TFChunk : IDisposable
    {
        public const int WriteBufferSize = 8192;
        public const int ReadBufferSize = 8192;

        #region Private Variables

        private static readonly ILogger _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(typeof(TFChunk));
        private static readonly ILogRecordParserProvider _logRecordParserProvider = ObjectContainer.Resolve<ILogRecordParserProvider>();

        private readonly string _filename;
        private int _dataPosition;
        private int _dataUnitSize;
        private int _dataCount;
        private volatile bool _isCompleted;
        private ChunkHeader _chunkHeader;
        private ChunkFooter _chunkFooter;

        private readonly int _readerCount;
        private readonly ConcurrentQueue<ReaderWorkItem> _readerWorkItemQueue = new ConcurrentQueue<ReaderWorkItem>();

        private WriterWorkItem _writerWorkItem;

        private readonly ManualResetEventSlim _destroyEvent = new ManualResetEventSlim(false);

        private IChunkReadSide _readSide;

        #endregion

        #region Public Properties

        public string FileName { get { return _filename; } }
        public ChunkHeader ChunkHeader { get { return _chunkHeader; } }
        public ChunkFooter ChunkFooter { get { return _chunkFooter; } }
        public bool IsCompleted { get { return _isCompleted; } }
        public int DataPosition { get { return _dataPosition; } }
        public long GlobalDataPosition
        {
            get
            {
                return ChunkHeader.ChunkDataStartPosition + DataPosition;
            }
        }

        #endregion

        #region Constructors

        private TFChunk(string filename, int readerCount)
        {
            Ensure.NotNullOrEmpty(filename, "filename");
            Ensure.Positive(readerCount, "readerCount");

            _filename = filename;
            _readerCount = readerCount;
        }
        private TFChunk(string filename, int dataUnitSize, int dataCount, int readerCount)
        {
            Ensure.NotNullOrEmpty(filename, "filename");
            Ensure.Positive(dataUnitSize, "dataUnitSize");
            Ensure.Positive(dataCount, "dataCount");
            Ensure.Positive(readerCount, "readerCount");

            _filename = filename;
            _dataUnitSize = dataUnitSize;
            _dataCount = dataCount;
            _readerCount = readerCount;
        }

        #endregion

        #region Factory Methods

        public static TFChunk CreateNew(string filename, int chunkNumber, TFChunkManagerConfig config)
        {
            TFChunk chunk;
            int chunkDataSize;

            if (config.ChunkDataUnitSize > 0 && config.ChunkDataCount > 0)
            {
                chunk = new TFChunk(filename, config.ChunkDataUnitSize, config.ChunkDataCount, config.ChunkReaderCount);
                chunkDataSize = config.ChunkDataUnitSize * config.ChunkDataCount;
            }
            else
            {
                chunk = new TFChunk(filename, config.ChunkReaderCount);
                chunkDataSize = config.ChunkDataSize;
            }

            var header = new ChunkHeader(chunkNumber, config.ChunkDataSize);
            var fileSize = ChunkHeader.Size + chunkDataSize + ChunkFooter.Size;

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
        public static TFChunk FromCompletedFile(string filename, TFChunkManagerConfig config)
        {
            TFChunk chunk;

            if (config.ChunkDataUnitSize > 0 && config.ChunkDataCount > 0)
            {
                chunk = new TFChunk(filename, config.ChunkDataUnitSize, config.ChunkDataCount, config.ChunkReaderCount);
            }
            else
            {
                chunk = new TFChunk(filename, config.ChunkReaderCount);
            }

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
        public static TFChunk FromOngoingFile(string filename, TFChunkManagerConfig config)
        {
            TFChunk chunk;

            if (config.ChunkDataUnitSize > 0 && config.ChunkDataCount > 0)
            {
                chunk = new TFChunk(filename, config.ChunkDataUnitSize, config.ChunkDataCount, config.ChunkReaderCount);
            }
            else
            {
                chunk = new TFChunk(filename, config.ChunkReaderCount);
            }

            try
            {
                chunk.InitOngoing();
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("Chunk {0} init from ongoing file failed.", chunk), ex);
                chunk.Dispose();
                throw;
            }

            return chunk;
        }

        #endregion

        #region Init Methods

        private void InitCompleted()
        {
            //先判断文件是否存在
            var fileInfo = new FileInfo(_filename);
            if (!fileInfo.Exists)
            {
                throw new CorruptDatabaseException(new ChunkNotFoundException(_filename));
            }

            //标记当前Chunk为已完成
            _isCompleted = true;

            //读取文件的头尾，检查文件是否有效
            using (var fileStream = new FileStream(_filename, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, ReadBufferSize, FileOptions.RandomAccess))
            {
                using (var reader = new BinaryReader(fileStream))
                {
                    //读取ChunkHeader和ChunkFooter
                    _chunkHeader = ReadHeader(fileStream, reader);
                    _chunkFooter = ReadFooter(fileStream, reader);

                    //检查Chunk文件的实际大小是否正确
                    var chunkDataTotalSize = _chunkFooter.ChunkDataTotalSize;
                    var chunkSize = ChunkHeader.Size + chunkDataTotalSize + ChunkFooter.Size;
                    if (chunkSize != fileStream.Length)
                    {
                        throw new CorruptDatabaseException(new BadChunkInDatabaseException(
                            string.Format("The size of chunk {0} should be equals with fileStream's length {1}, but instead it was {2}.",
                                            this,
                                            fileStream.Length,
                                            chunkSize)));
                    }

                    //如果Chunk中的数据是固定大小的，则还需要检查数据总数是否正确
                    if (IsFixedDataSize())
                    {
                        var expectedTotalDataSize = GetExpectedTotalFixedDataSize();
                        if (chunkDataTotalSize != expectedTotalDataSize)
                        {
                            throw new CorruptDatabaseException(new BadChunkInDatabaseException(
                                string.Format("The total data size of chunk {0} should be {1}, but instead it was {2}.",
                                                this,
                                                expectedTotalDataSize,
                                                _chunkFooter.ChunkDataTotalSize)));
                        }
                    }
                }
            }

            _dataPosition = _chunkFooter.ChunkDataTotalSize;

            //初始化文件属性以及创建读文件的Reader
            SetAttributes();
            InitializeReaderWorkItems();

            _readSide = new TFChunkReadSideUnscavenged(this);
        }
        private void InitNew(ChunkHeader chunkHeader, int fileSize)
        {
            Ensure.NotNull(chunkHeader, "chunkHeader");
            Ensure.Positive(fileSize, "fileSize");

            _chunkHeader = chunkHeader;

            //标记当前Chunk为未完成
            _isCompleted = false;

            //先创建临时文件，并将Chunk Header写入临时文件
            var tempFilename = string.Format("{0}.{1}.tmp", _filename, Guid.NewGuid());
            var tempFileStream = new FileStream(tempFilename, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read, WriteBufferSize, FileOptions.SequentialScan);
            tempFileStream.SetLength(fileSize);
            tempFileStream.Write(chunkHeader.AsByteArray(), 0, ChunkHeader.Size);
            tempFileStream.FlushToDisk();
            tempFileStream.Close();

            //将临时文件移动到正式的位置
            File.Move(tempFilename, _filename);

            //创建写文件的Writer
            var fileStream = new FileStream(_filename, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, WriteBufferSize, FileOptions.SequentialScan);
            fileStream.Position = ChunkHeader.Size;
            _dataPosition = 0;
            _writerWorkItem = new WriterWorkItem(fileStream);

            //初始化文件属性以及创建读文件的Reader
            SetAttributes();
            InitializeReaderWorkItems();

            _readSide = new TFChunkReadSideUnscavenged(this);
        }
        private void InitOngoing()
        {
            //先判断文件是否存在
            var fileInfo = new FileInfo(_filename);
            if (!fileInfo.Exists)
            {
                throw new CorruptDatabaseException(new ChunkNotFoundException(_filename));
            }

            //标记当前Chunk为未完成
            _isCompleted = false;

            //读取ChunkHeader和并设置下一个数据的文件流写入起始位置
            using (var fileStream = new FileStream(_filename, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, ReadBufferSize, FileOptions.SequentialScan))
            {
                using (var reader = new BinaryReader(fileStream))
                {
                    _chunkHeader = ReadHeader(fileStream, reader);
                    SetStreamWriteStartPosition(fileStream, reader);
                    _dataPosition = (int)fileStream.Position - ChunkHeader.Size;
                }
            }

            //创建写文件的Writer
            var writeFileStream = new FileStream(_filename, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, WriteBufferSize, FileOptions.SequentialScan);
            writeFileStream.Position = _dataPosition + ChunkHeader.Size;
            _writerWorkItem = new WriterWorkItem(writeFileStream);

            //初始化文件属性以及创建读文件的Reader
            SetAttributes();
            InitializeReaderWorkItems();

            _readSide = new TFChunkReadSideUnscavenged(this);

            _logger.InfoFormat("Ongoing chunk {0} initialized, write data start position: {1}", this, _dataPosition);
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
            if (_isCompleted)
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

            if (writerWorkItem.FileStream.Position + recordLength + 2 * sizeof(int) > ChunkHeader.Size + _chunkHeader.ChunkDataTotalSize)
            {
                return RecordWriteResult.NotEnoughSpace(_dataPosition);
            }

            var oldPosition = _dataPosition;
            var buffer = bufferStream.GetBuffer();
            writerWorkItem.AppendData(buffer, 0, (int)bufferStream.Length);
            _dataPosition = (int)writerWorkItem.FileStream.Position - ChunkHeader.Size;

            return RecordWriteResult.Successful(oldPosition, _dataPosition);
        }

        #endregion

        #region Complete Methods

        public void Flush()
        {
            if (_isCompleted)
            {
                throw new InvalidOperationException(string.Format("Cannot flush a read-only TFChunk: {0}", this));
            }

            _writerWorkItem.FlushToDisk();
        }
        public void Complete()
        {
            if (_isCompleted)
            {
                throw new ChunkCompleteException(string.Format("Cannot complete a read-only TFChunk: {0}", this));
            }

            _chunkFooter = WriteFooter();
            Flush();
            _isCompleted = true;

            _writerWorkItem.Dispose();
            _writerWorkItem = null;
            SetAttributes();
        }

        #endregion

        #region Clean Methods

        public void Dispose()
        {
            Close();
        }
        public void Close()
        {
            _logger.InfoFormat("Chunk {0} is closing.", this);
            if (!_isCompleted)
            {
                _logger.InfoFormat("Chunk {0} try to flush data.", this);
                Flush();
                var localDataPosition = _dataPosition;
                var globalDataPosition = ChunkHeader.ChunkDataStartPosition + localDataPosition;
                _logger.InfoFormat("Chunk {0} flush data success, localDataPosition: {1}, globalDataPosition: {2}", this, localDataPosition, globalDataPosition);
            }
            _logger.InfoFormat("Chunk {0} closed.", this);
        }
        public void MarkForDeletion()
        {
            //_selfdestructin54321 = true;
            //_deleteFile = true;
            TryDestructFileStreams();
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
            //int fileStreamCount = int.MaxValue;

            //ReaderWorkItem workItem;
            //while (_readerWorkItemQueue.TryDequeue(out workItem))
            //{
            //    workItem.Stream.Dispose();
            //    fileStreamCount = Interlocked.Decrement(ref _fileStreamCount);
            //}

            //if (fileStreamCount < 0)
            //{
            //    throw new Exception("Somehow we managed to decrease count of file streams below zero.");
            //}
            //if (fileStreamCount == 0) // we are the last who should "turn the light off" for file streams
            //{
            //    CleanFileStreamDestruction();
            //}
        }
        private void CleanFileStreamDestruction()
        {
            if (_writerWorkItem != null)
            {
                _writerWorkItem.Dispose();
            }

            Helper.EatException(() => File.SetAttributes(_filename, FileAttributes.Normal));

            //if (_deleteFile)
            //{
            //    _logger.InfoFormat("File {0} has been marked for delete and will be deleted in TryDestructFileStreams.", Path.GetFileName(_filename));
            //    Helper.EatException(() => File.Delete(_filename));
            //}

            _destroyEvent.Set();
        }

        #endregion

        #region Helper Methods

        private T ReadChunk<T>(Func<ReaderWorkItem, T> readFunc)
        {
            var reader = GetReaderWorkItem();
            try
            {
                return readFunc(reader);
            }
            finally
            {
                ReturnReaderWorkItem(reader);
            }
        }
        private void ReadChunk(Action<ReaderWorkItem> readAction)
        {
            var reader = GetReaderWorkItem();
            try
            {
                readAction(reader);
            }
            finally
            {
                ReturnReaderWorkItem(reader);
            }
        }
        private void InitializeReaderWorkItems()
        {
            for (var i = 0; i < _readerCount; i++)
            {
                _readerWorkItemQueue.Enqueue(CreateReaderWorkItem());
            }
        }
        private ReaderWorkItem CreateReaderWorkItem()
        {
            var fileStream = new FileStream(_filename, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, ReadBufferSize, FileOptions.RandomAccess);
            var reader = new BinaryReader(fileStream);
            return new ReaderWorkItem(fileStream, reader);
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

        private ChunkFooter WriteFooter()
        {
            var currentTotalDataSize = DataPosition;

            //如果是固定大小的数据，则检查总数据大小是否正确
            if (IsFixedDataSize())
            {
                var expectTotalDataSize = GetExpectedTotalFixedDataSize();
                if (DataPosition != expectTotalDataSize)
                {
                    throw new ChunkCompleteException(string.Format("Cannot write the chunk footer as the current total data size is incorrect. chunk: {0}, expectTotalDataSize: {1}, currentTotalDataSize: {2}",
                        this,
                        expectTotalDataSize,
                        currentTotalDataSize));
                }
            }

            var workItem = _writerWorkItem;
            var footer = new ChunkFooter(currentTotalDataSize);

            workItem.AppendData(footer.AsByteArray(), 0, ChunkFooter.Size);

            Flush(); // trying to prevent bug with resized file, but no data in it

            var oldStreamLength = workItem.FileStream.Length;
            var newStreamLength = ChunkHeader.Size + currentTotalDataSize + ChunkFooter.Size;

            if (newStreamLength != oldStreamLength)
            {
                workItem.ResizeStream(newStreamLength);
            }

            return footer;
        }
        private ChunkHeader ReadHeader(FileStream stream, BinaryReader reader)
        {
            if (stream.Length < ChunkHeader.Size)
            {
                throw new Exception(string.Format("Chunk file '{0}' is too short to even read ChunkHeader, its size is {1} bytes.", _filename, stream.Length));
            }
            stream.Seek(0, SeekOrigin.Begin);
            return ChunkHeader.FromStream(reader, stream);
        }
        private ChunkFooter ReadFooter(FileStream stream, BinaryReader reader)
        {
            if (stream.Length < ChunkFooter.Size)
            {
                throw new Exception(string.Format("Chunk file '{0}' is too short to even read ChunkFooter, its size is {1} bytes.", _filename, stream.Length));
            }
            stream.Seek(-ChunkFooter.Size, SeekOrigin.End);
            return ChunkFooter.FromStream(reader, stream);
        }

        private bool IsFixedDataSize()
        {
            return _dataUnitSize > 0 && _dataCount > 0;
        }
        private int GetExpectedTotalFixedDataSize()
        {
            var unitSize = _dataUnitSize + 2 * sizeof(int);
            return unitSize * _dataCount;
        }
        private void SetStreamWriteStartPosition(FileStream stream, BinaryReader reader)
        {
            stream.Position = ChunkHeader.Size;

            var startStreamPosition = stream.Position;
            var maxStreamPosition = stream.Length - ChunkFooter.Size;

            while (stream.Position < maxStreamPosition)
            {
                if (TryReadRecord(stream, reader, maxStreamPosition))
                {
                    startStreamPosition = stream.Position;
                }
                else
                {
                    break;
                }
            }

            if (startStreamPosition != stream.Position)
            {
                stream.Position = startStreamPosition;
            }
        }
        private bool TryReadRecord(FileStream stream, BinaryReader reader, long maxStreamPosition)
        {
            try
            {
                var startStreamPosition = stream.Position;

                if (startStreamPosition + 2 * sizeof(int) > maxStreamPosition)
                {
                    return false;
                }
                var length = reader.ReadInt32();
                if (length <= 0 || length > Consts.MaxLogRecordSize)
                {
                    return false;
                }
                if (IsFixedDataSize() && length != _dataUnitSize)
                {
                    _logger.ErrorFormat("Invalid fixed data length, expected length {0}, but was {1}", _dataUnitSize, length);
                    return false;
                }
                if (startStreamPosition + length + 2 * sizeof(int) > maxStreamPosition)
                {
                    return false;
                }

                var recordType = reader.ReadByte();
                var recordParser = _logRecordParserProvider.GetLogRecordParser(recordType);
                var record = recordParser.ParseFrom(reader);
                if (record == null)
                {
                    return false;
                }

                int suffixLength = reader.ReadInt32();
                if (suffixLength != length)
                {
                    return false;
                }

                return true;
            }
            catch
            {
                return false;
            }
        }
        private static long GetStreamPosition(long dataPosition)
        {
            return ChunkHeader.Size + dataPosition;
        }

        private void SetAttributes()
        {
            Helper.EatException(() =>
            {
                if (_isCompleted)
                {
                    File.SetAttributes(_filename, FileAttributes.ReadOnly | FileAttributes.NotContentIndexed);
                }
                else
                {
                    File.SetAttributes(_filename, FileAttributes.NotContentIndexed);
                }
            });
        }

        #endregion

        public override string ToString()
        {
            return string.Format("#{0} ({1})", _chunkHeader.ChunkNumber, Path.GetFileName(_filename));
        }
    }
}
