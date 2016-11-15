using System.IO;
using System.Linq;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Serializing;
using ECommon.Storage;
using EQueue.Protocols.Brokers;

namespace EQueue.Broker
{
    public interface IQueue
    {
        string Topic { get; }
        int QueueId { get; }
        long NextOffset { get; }
        long IncrementNextOffset();
    }
    public class Queue : IQueue
    {
        private const string QueueSettingFileName = "queue.setting";
        private readonly ChunkWriter _chunkWriter;
        private readonly ChunkReader _chunkReader;
        private readonly ChunkManager _chunkManager;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly string _queueSettingFile;
        private QueueSetting _setting;
        private long _nextOffset = 0;
        private ILogger _logger;

        public string Topic { get; private set; }
        public int QueueId { get; private set; }
        public long NextOffset { get { return _nextOffset; } }
        public QueueSetting Setting { get { return _setting; } }
        public QueueKey Key { get; private set; }

        public Queue(string topic, int queueId)
        {
            Topic = topic;
            QueueId = queueId;
            Key = new QueueKey(topic, queueId);

            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _chunkManager = new ChunkManager("QueueChunk-" + Key.ToString(), BrokerController.Instance.Setting.QueueChunkConfig, BrokerController.Instance.Setting.IsMessageStoreMemoryMode, Topic + @"\" + QueueId);
            _chunkWriter = new ChunkWriter(_chunkManager);
            _chunkReader = new ChunkReader(_chunkManager, _chunkWriter);
            _queueSettingFile = Path.Combine(_chunkManager.ChunkPath, QueueSettingFileName);
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(this.GetType().FullName);
        }

        public void Load()
        {
            _setting = LoadQueueSetting();
            if (_setting == null)
            {
                _setting = new QueueSetting();
                SaveQueueSetting();
            }
            if (_setting.IsDeleted)
            {
                return;
            }
            _chunkManager.Load(ReadMessageIndex);
            _chunkWriter.Open();

            var lastChunk = _chunkManager.GetLastChunk();
            var lastOffsetGlobalPosition = lastChunk.DataPosition + lastChunk.ChunkHeader.ChunkDataStartPosition;
            if (lastOffsetGlobalPosition > 0)
            {
                _nextOffset = lastOffsetGlobalPosition / _chunkManager.Config.ChunkDataUnitSize;
            }
        }
        public void Close()
        {
            _chunkWriter.Close();
            _chunkManager.Close();
        }
        public void AddMessage(long messagePosition, string messageTag)
        {
            _chunkWriter.Write(new QueueLogRecord(messagePosition + 1, messageTag.GetStringHashcode()));
        }
        public long GetMessagePosition(long queueOffset, out int tagCode, bool autoCache = true)
        {
            tagCode = 0;

            var position = queueOffset * _chunkManager.Config.ChunkDataUnitSize;
            var record = _chunkReader.TryReadAt(position, ReadMessageIndex, autoCache);
            if (record == null)
            {
                return -1L;
            }

            tagCode = record.TagCode;
            return record.MessageLogPosition - 1;
        }
        public void SetProducerVisible(bool visible)
        {
            _setting.ProducerVisible = visible;
            SaveQueueSetting();
        }
        public void SetConsumerVisible(bool visible)
        {
            _setting.ConsumerVisible = visible;
            SaveQueueSetting();
        }
        public void Delete()
        {
            _setting.IsDeleted = true;
            SaveQueueSetting();

            Close();

            if (!_chunkManager.IsMemoryMode)
            {
                Directory.Delete(_chunkManager.ChunkPath, true);
            }
        }
        public long IncrementNextOffset()
        {
            return _nextOffset++;
        }
        public long GetMinQueueOffset()
        {
            if (_nextOffset == 0L)
            {
                return -1L;
            }
            return _chunkManager.GetFirstChunk().ChunkHeader.ChunkDataStartPosition / _chunkManager.Config.ChunkDataUnitSize;
        }
        public void DeleteMessages(long minMessagePosition)
        {
            var chunks = _chunkManager.GetAllChunks().Where(x => x.IsCompleted).OrderBy(x => x.ChunkHeader.ChunkNumber);

            foreach (var chunk in chunks)
            {
                var maxPosition = chunk.ChunkHeader.ChunkDataEndPosition - _chunkManager.Config.ChunkDataUnitSize;
                var record = _chunkReader.TryReadAt(maxPosition, ReadMessageIndex, false);
                if (record == null)
                {
                    continue;
                }
                var chunkLastMessagePosition = record.MessageLogPosition - 1;
                if (chunkLastMessagePosition < minMessagePosition)
                {
                    if (_chunkManager.RemoveChunk(chunk))
                    {
                        _logger.InfoFormat("Queue (topic: {0}, queueId: {1}) chunk #{2} is deleted, chunkLastMessagePosition: {3}, messageStoreMinMessagePosition: {4}", Topic, QueueId, chunk.ChunkHeader.ChunkNumber, chunkLastMessagePosition, minMessagePosition);
                    }
                }
            }
        }

        private QueueLogRecord ReadMessageIndex(byte[] recordBuffer)
        {
            var record = new QueueLogRecord();
            record.ReadFrom(recordBuffer);
            if (record.MessageLogPosition <= 0)
            {
                return null;
            }
            return record;
        }
        private QueueSetting LoadQueueSetting()
        {
            if (_chunkManager.IsMemoryMode)
            {
                return null;
            }
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
            if (_chunkManager.IsMemoryMode)
            {
                return;
            }
            if (!Directory.Exists(_chunkManager.ChunkPath))
            {
                Directory.CreateDirectory(_chunkManager.ChunkPath);
            }
            using (var stream = new FileStream(_queueSettingFile, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite))
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
        public bool ProducerVisible;
        public bool ConsumerVisible;
        public bool IsDeleted;

        public QueueSetting()
        {
            ProducerVisible = true;
            ConsumerVisible = true;
            IsDeleted = false;
        }
    }
}
