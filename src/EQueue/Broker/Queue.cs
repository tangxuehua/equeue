using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.Serializing;
using EQueue.Broker.Storage;

namespace EQueue.Broker
{
    public class Queue
    {
        private const string QueueSettingFileName = "queue.setting";
        private readonly TFChunkWriter _chunkWriter;
        private readonly TFChunkReader _chunkReader;
        private readonly TFChunkManager _chunkManager;
        private ConcurrentDictionary<long, long> _queueItemDict = new ConcurrentDictionary<long, long>();
        private long _currentOffset = 0;
        private readonly IJsonSerializer _jsonSerializer;
        private QueueSetting _setting;
        private readonly string _queueSettingFile;

        public string Topic { get; private set; }
        public int QueueId { get; private set; }
        public long CurrentOffset { get { return _currentOffset; } }
        public QueueSetting Setting { get { return _setting; } }

        public Queue(string topic, int queueId)
        {
            Topic = topic;
            QueueId = queueId;

            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _chunkManager = new TFChunkManager(BrokerController.Instance.Setting.QueueChunkConfig, ReadMessageIndex, Topic + @"\" + QueueId);
            _chunkWriter = new TFChunkWriter(_chunkManager);
            _chunkReader = new TFChunkReader(_chunkManager, _chunkWriter);
            _queueSettingFile = Path.Combine(_chunkManager.ChunkPath, QueueSettingFileName);
        }

        public void Load()
        {
            _setting = LoadQueueSetting();
            if (_setting == null)
            {
                _setting = new QueueSetting { Status = QueueStatus.Normal };
                SaveQueueSetting();
            }
            _chunkManager.Load();
            _chunkWriter.Open();
        }
        public void Close()
        {
            _chunkWriter.Close();
            _chunkManager.Close();
        }
        public void AddMessage(long messageLogPosition)
        {
            _chunkWriter.Write(new QueueLogRecord(messageLogPosition));
        }
        public long GetMessageOffset(long queueOffset)
        {
            var position = queueOffset * _chunkManager.Config.ChunkDataUnitSize;
            var result = _chunkReader.TryReadAt(position);
            if (result.Success)
            {
                return ((QueueLogRecord)result.LogRecord).MessageLogPosition;
            }
            return -1L;
        }
        public void Enable()
        {
            _setting.Status = QueueStatus.Normal;
            SaveQueueSetting();
        }
        public void Disable()
        {
            _setting.Status = QueueStatus.Disabled;
            SaveQueueSetting();
        }
        public long IncrementCurrentOffset()
        {
            return _currentOffset++;
        }
        public long GetMinQueueOffset()
        {
            return _chunkManager.GetFirstChunk().ChunkHeader.ChunkDataStartPosition / _chunkManager.Config.ChunkDataUnitSize;
        }
        public long RemoveAllPreviousQueueIndex(long maxAllowToRemoveQueueOffset)
        {
            var totalRemovedCount = 0L;
            var allPreviousQueueOffsets = _queueItemDict.Keys.Where(key => key <= maxAllowToRemoveQueueOffset);
            foreach (var queueOffset in allPreviousQueueOffsets)
            {
                long messageOffset;
                if (_queueItemDict.TryRemove(queueOffset, out messageOffset))
                {
                    totalRemovedCount++;
                }
            }
            return totalRemovedCount;
        }
        public long RemoveRequiredQueueIndexFromLast(long requireRemoveCount)
        {
            var queueOffset = _queueItemDict.Keys.LastOrDefault();
            var totalRemovedCount = 0L;
            while (queueOffset >= 0L && totalRemovedCount < requireRemoveCount)
            {
                long messageOffset;
                if (_queueItemDict.TryRemove(queueOffset, out messageOffset))
                {
                    totalRemovedCount++;
                }
                queueOffset--;
            }
            return totalRemovedCount;
        }

        private long GetChunkMessageCount(TFChunk chunk)
        {
            if (chunk.IsCompleted)
            {
                return chunk.ChunkFooter.ChunkDataTotalSize / _chunkManager.Config.ChunkDataUnitSize;
            }
            else
            {
                return chunk.DataPosition / _chunkManager.Config.ChunkDataUnitSize;
            }
        }
        private ILogRecord ReadMessageIndex(BinaryReader reader)
        {
            var record = new QueueLogRecord();
            record.ReadFrom(reader);
            return record;
        }
        private QueueSetting LoadQueueSetting()
        {
            if (!Directory.Exists(_chunkManager.ChunkPath))
            {
                Directory.CreateDirectory(_chunkManager.ChunkPath);
            }
            using (var stream = new FileStream(_queueSettingFile, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                using (var reader = new StreamReader(stream))
                {
                    var text = reader.ReadToEnd();
                    if (!string.IsNullOrEmpty(text))
                    {
                        return _jsonSerializer.Deserialize<QueueSetting>(text);
                    }
                    return null;
                }
            }
        }
        private void SaveQueueSetting()
        {
            if (!Directory.Exists(_chunkManager.ChunkPath))
            {
                Directory.CreateDirectory(_chunkManager.ChunkPath);
            }
            using (var stream = new FileStream(_queueSettingFile, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                using (var writer = new StreamWriter(stream))
                {
                    writer.Write(_jsonSerializer.Serialize(_setting));
                }
            }
        }
    }
    public class QueueSetting
    {
        public QueueStatus Status;
    }
}
