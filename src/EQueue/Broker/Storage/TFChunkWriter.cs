using ECommon.Components;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Utilities;

namespace EQueue.Broker.Storage
{
    public class TFChunkWriter
    {
        public TFChunk CurrentChunk { get { return _currentChunk; } }

        private readonly TFChunkManager _chunkManager;
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;
        private readonly object _lockObj = new object();
        private bool _isClosed = false;

        private TFChunk _currentChunk;

        public TFChunkWriter(TFChunkManager chunkManager)
        {
            Ensure.NotNull(chunkManager, "chunkManager");

            _chunkManager = chunkManager;
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(typeof(TFChunkWriter).FullName);
        }

        public void Open()
        {
            _currentChunk = _chunkManager.GetLastChunk();
            _scheduleService.StartTask("TFChunkWriter.Flush", Flush, 1000, _chunkManager.Config.FlushChunkIntervalMilliseconds);
        }
        public long Write(ILogRecord record)
        {
            lock (_lockObj)
            {
                if (_isClosed) return -1;

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
                _scheduleService.StopTask("TFChunkWriter.Flush");
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
