using System;
using System.IO;
using ECommon.Logging;
using ECommon.Scheduling;
using EQueue.Broker.DeleteMessageStrategies;
using EQueue.Broker.Storage;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class DefaultMessageStore : IMessageStore, IDisposable
    {
        private TFChunkManager _chunkManager;
        private TFChunkWriter _chunkWriter;
        private TFChunkReader _chunkReader;
        private readonly IDeleteMessageStrategy _deleteMessageStragegy;
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;
        private long _minConsumedMessagePosition;

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

        public DefaultMessageStore(IDeleteMessageStrategy deleteMessageStragegy, IScheduleService scheduleService, ILoggerFactory loggerFactory)
        {
            _deleteMessageStragegy = deleteMessageStragegy;
            _scheduleService = scheduleService;
            _logger = loggerFactory.Create(GetType().FullName);
        }

        public void Start()
        {
            _chunkManager = new TFChunkManager(this.GetType().Name, BrokerController.Instance.Setting.MessageChunkConfig);
            _chunkWriter = new TFChunkWriter(_chunkManager);
            _chunkReader = new TFChunkReader(_chunkManager, _chunkWriter);

            _chunkManager.Load(ReadMessage);
            _chunkWriter.Open();

            _scheduleService.StartTask(string.Format("{0}.DeleteMessages", this.GetType().Name), DeleteMessages, 5 * 1000, BrokerController.Instance.Setting.DeleteMessagesInterval);
        }
        public void Shutdown()
        {
            _scheduleService.StopTask(string.Format("{0}.DeleteMessages", this.GetType().Name));
            _chunkWriter.Close();
            _chunkManager.Close();
        }
        public MessageLogRecord StoreMessage(int queueId, long queueOffset, Message message, string routingKey)
        {
            var record = new MessageLogRecord(
                message.Topic,
                message.Code,
                message.Key,
                message.Body,
                queueId,
                queueOffset,
                routingKey,
                message.CreatedTime,
                DateTime.Now);
            _chunkWriter.Write(record);
            return record;
        }
        public byte[] GetMessage(long position)
        {
            return _chunkReader.TryReadRecordBufferAt(position).RecordBuffer;
        }
        public void UpdateMinConsumedMessagePosition(long minConsumedMessagePosition)
        {
            if (_minConsumedMessagePosition == 0 && minConsumedMessagePosition > 0)
            {
                _minConsumedMessagePosition = minConsumedMessagePosition;
            }
            else if (_minConsumedMessagePosition < minConsumedMessagePosition)
            {
                _minConsumedMessagePosition = minConsumedMessagePosition;
            }
        }

        private void DeleteMessages()
        {
            var chunks = _deleteMessageStragegy.GetAllowDeleteChunks(_chunkManager, _minConsumedMessagePosition);
            foreach (var chunk in chunks)
            {
                if (_chunkManager.RemoveChunk(chunk))
                {
                    _logger.InfoFormat("Message chunk {0} is deleted.", chunk);
                }
            }
        }
        private MessageLogRecord ReadMessage(int length, BinaryReader reader)
        {
            var record = new MessageLogRecord();
            record.ReadFrom(length, reader);
            return record;
        }

        public void Dispose()
        {
            Shutdown();
        }
    }
}
