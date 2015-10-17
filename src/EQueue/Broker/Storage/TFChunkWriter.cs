using ECommon.Components;
using ECommon.Scheduling;
using ECommon.Utilities;
using EQueue.Broker.Storage.LogRecords;

namespace EQueue.Broker.Storage
{
    public class TFChunkWriter
    {
        public TFChunk CurrentChunk { get { return _currentChunk; } }

        private readonly TFChunkManager _chunkManager;
        private readonly IScheduleService _scheduleService;
        private readonly object _lockObj = new object();
        private bool _isClosed = false;
        private readonly string _flushTaskName;

        private TFChunk _currentChunk;

        public TFChunkWriter(TFChunkManager chunkManager)
        {
            Ensure.NotNull(chunkManager, "chunkManager");

            _chunkManager = chunkManager;
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _flushTaskName = string.Format("{0}-FlushChunk", _chunkManager.Name);
        }

        public void Open()
        {
            _currentChunk = _chunkManager.GetLastChunk();
            _scheduleService.StartTask(_flushTaskName, Flush, 1000, _chunkManager.Config.FlushChunkIntervalMilliseconds);
        }
        public long Write(ILogRecord record)
        {
            lock (_lockObj)
            {
                if (_isClosed)
                {
                    throw new ChunkWriteException(_currentChunk.ToString(), "Chunk writer is closed.");
                }

                //如果当前文件已经写完，则需要新建文件
                if (_currentChunk.IsCompleted)
                {
                    _currentChunk = _chunkManager.AddNewChunk();
                }

                //先尝试写文件
                var result = _currentChunk.TryAppend(record);

                //如果当前文件已满
                if (!result.Success)
                {
                    //结束当前文件
                    _currentChunk.Complete();

                    //新建新的文件
                    _currentChunk = _chunkManager.AddNewChunk();

                    //再尝试写入新的文件
                    result = _currentChunk.TryAppend(record);

                    //如果还是不成功，则报错
                    if (!result.Success)
                    {
                        throw new ChunkWriteException(_currentChunk.ToString(), "Write record to chunk failed.");
                    }
                }

                //返回数据写入位置
                return result.Position;
            }
        }
        public void Close()
        {
            lock (_lockObj)
            {
                _scheduleService.StopTask(_flushTaskName);
                _isClosed = true;
            }
        }
        private void Flush()
        {
            lock (_lockObj)
            {
                if (_isClosed) return;
                if (_currentChunk.IsCompleted) return;

                _currentChunk.Flush();
            }
        }
    }
}
