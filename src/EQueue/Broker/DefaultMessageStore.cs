using System;
using ECommon.Logging;
using ECommon.Scheduling;
using EQueue.Broker.DeleteMessageStrategies;
using EQueue.Broker.Storage;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Broker
{
    public class DefaultMessageStore : IMessageStore, IDisposable
    {
        private ChunkManager _chunkManager;
        private ChunkWriter _chunkWriter;
        private ChunkReader _chunkReader;
        private readonly IDeleteMessageStrategy _deleteMessageStragegy;
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;
        private long _minConsumedMessagePosition = -1;
        private BufferQueue<MessageLogRecord> _bufferQueue;
        private readonly object _lockObj = new object();

        public long MinMessagePosition
        {
            get
            {
                return _chunkManager.GetFirstChunk().ChunkHeader.ChunkDataStartPosition;
            }
        }
        public long CurrentMessagePosition
        {
            get
            {
                return _chunkWriter.CurrentChunk.GlobalDataPosition;
            }
        }
        public int ChunkCount
        {
            get { return _chunkManager.GetChunkCount(); }
        }
        public int MinChunkNum
        {
            get { return _chunkManager.GetFirstChunk().ChunkHeader.ChunkNumber; }
        }
        public int MaxChunkNum
        {
            get { return _chunkManager.GetLastChunk().ChunkHeader.ChunkNumber; }
        }

        public DefaultMessageStore(IDeleteMessageStrategy deleteMessageStragegy, IScheduleService scheduleService, ILoggerFactory loggerFactory)
        {
            _deleteMessageStragegy = deleteMessageStragegy;
            _scheduleService = scheduleService;
            _logger = loggerFactory.Create(GetType().FullName);
        }

        public void Load()
        {
            var messageWriteQueueThreshold = BrokerController.Instance.Setting.MessageWriteQueueThreshold;
            _bufferQueue = new BufferQueue<MessageLogRecord>("MessageBufferQueue", messageWriteQueueThreshold, PersistMessages, _logger);
            _chunkManager = new ChunkManager(this.GetType().Name, BrokerController.Instance.Setting.MessageChunkConfig);
            _chunkWriter = new ChunkWriter(_chunkManager);
            _chunkReader = new ChunkReader(_chunkManager, _chunkWriter);

            _chunkManager.Load(ReadMessage);
        }
        public void Start()
        {
            _chunkWriter.Open();
            _scheduleService.StartTask("DeleteMessages", DeleteMessages, 5 * 1000, BrokerController.Instance.Setting.DeleteMessagesInterval);
            _bufferQueue.Start();
        }
        public void Shutdown()
        {
            _bufferQueue.Stop();
            _scheduleService.StopTask("DeleteMessages");
            _chunkWriter.Close();
            _chunkManager.Close();
        }
        public void StoreMessageAsync(IQueue queue, Message message, Action<MessageLogRecord, object> callback, object parameter)
        {
            lock (_lockObj)
            {
                var record = new MessageLogRecord(
                    message.Topic,
                    message.Code,
                    message.Body,
                    queue.QueueId,
                    queue.NextOffset,
                    message.CreatedTime,
                    DateTime.Now,
                    message.Tag,
                    callback,
                    parameter);
                _bufferQueue.EnqueueMessage(record);
                queue.IncrementNextOffset();
            }
        }
        public byte[] GetMessageBuffer(long position)
        {
            var record = _chunkReader.TryReadRecordBufferAt(position);
            if (record != null)
            {
                return record.RecordBuffer;
            }
            return null;
        }
        public QueueMessage GetMessage(long position)
        {
            var buffer = GetMessageBuffer(position);
            if (buffer != null)
            {
                var nextOffset = 0;
                var messageLength = MessageUtils.DecodeInt(buffer, nextOffset, out nextOffset);
                if (messageLength > 0)
                {
                    var message = new QueueMessage();
                    var messageBytes = new byte[messageLength];
                    Buffer.BlockCopy(buffer, nextOffset, messageBytes, 0, messageLength);
                    message.ReadFrom(messageBytes);
                    return message;
                }
            }
            return null;
        }
        public bool IsMessagePositionExist(long position)
        {
            var chunk = _chunkManager.GetChunkFor(position);
            return chunk != null;
        }
        public void UpdateMinConsumedMessagePosition(long minConsumedMessagePosition)
        {
            _minConsumedMessagePosition = minConsumedMessagePosition;
        }

        private void PersistMessages(MessageLogRecord message)
        {
            _chunkWriter.Write(message);
            message.OnPersisted();
        }

        private void DeleteMessages()
        {
            var chunks = _deleteMessageStragegy.GetAllowDeleteChunks(_chunkManager, _minConsumedMessagePosition);
            foreach (var chunk in chunks)
            {
                if (_chunkManager.RemoveChunk(chunk))
                {
                    _logger.InfoFormat("Message chunk #{0} is deleted, chunkPositionScale: [{1}, {2}], minConsumedMessagePosition: {3}",
                        chunk.ChunkHeader.ChunkNumber,
                        chunk.ChunkHeader.ChunkDataStartPosition,
                        chunk.ChunkHeader.ChunkDataEndPosition,
                        _minConsumedMessagePosition);
                }
            }
        }
        private MessageLogRecord ReadMessage(byte[] recordBuffer)
        {
            var record = new MessageLogRecord();
            record.ReadFrom(recordBuffer);
            return record;
        }

        public void Dispose()
        {
            Shutdown();
        }
    }
}
