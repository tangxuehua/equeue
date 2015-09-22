using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Scheduling;
using EQueue.Broker.Storage;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class DefaultMessageStore : IMessageStore, IDisposable
    {
        private TFChunkManager _chunkManager;
        private TFChunkWriter _chunkWriter;
        private TFChunkReader _chunkReader;
        private readonly ConcurrentDictionary<string, long> _queueConsumedOffsetDict = new ConcurrentDictionary<string, long>();
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;

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

        public DefaultMessageStore()
        {
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void Start()
        {
            _chunkManager = new TFChunkManager(this.GetType().Name, BrokerController.Instance.Setting.MessageChunkConfig, ReadMessage);
            _chunkWriter = new TFChunkWriter(_chunkManager);
            _chunkReader = new TFChunkReader(_chunkManager, _chunkWriter);

            _chunkManager.Load();
            _chunkWriter.Open();

            _scheduleService.StartTask(string.Format("{0}.DeleteMessages", this.GetType().Name), DeleteMessages, 5 * 1000, BrokerController.Instance.Setting.DeleteMessagesInterval);
        }
        public void Shutdown()
        {
            _scheduleService.StopTask(string.Format("{0}.DeleteMessages", this.GetType().Name));
            _chunkWriter.Close();
            _chunkManager.Close();
        }
        public long StoreMessage(int queueId, long queueOffset, Message message, string routingKey)
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
            return _chunkWriter.Write(record);
        }
        public MessageLogRecord GetMessage(long position)
        {
            var result = _chunkReader.TryReadAt(position);
            if (result.Success)
            {
                return result.LogRecord as MessageLogRecord;
            }
            return null;
        }
        public QueueMessage FindMessage(long? offset, string messageId)
        {
            return null;
            //TODO
            //var predicate = new Func<QueueMessage, bool>(x =>
            //{
            //    var pass = true;
            //    if (pass && offset != null)
            //    {
            //        pass = x.MessageOffset == offset.Value;
            //    }
            //    if (!string.IsNullOrWhiteSpace(messageId))
            //    {
            //        pass = x.MessageId == messageId;
            //    }
            //    return pass;
            //});
            //return _messageDict.Values.SingleOrDefault(predicate);
        }
        public void DeleteQueueMessage(string topic, int queueId)
        {
            //TODO
            //var key = string.Format("{0}-{1}", topic, queueId);
            //var messages = _messageDict.Values.Where(x => x.Topic == topic && x.QueueId == queueId);
            //messages.ForEach(x => _messageDict.Remove(x.MessageOffset));
            //_queueConsumedOffsetDict.Remove(key);
        }
        public void UpdateConsumedQueueOffset(string topic, int queueId, long queueOffset)
        {
            var key = string.Format("{0}-{1}", topic, queueId);
            _queueConsumedOffsetDict.AddOrUpdate(key, queueOffset, (currentKey, oldOffset) => queueOffset > oldOffset ? queueOffset : oldOffset);
        }

        private void DeleteMessages()
        {
            //TODO
            //var queueMessages = _messageDict.Values;
            //foreach (var queueMessage in queueMessages)
            //{
            //    var key = string.Format("{0}-{1}", queueMessage.Topic, queueMessage.QueueId);
            //    long maxAllowToDeleteQueueOffset;
            //    if (_queueConsumedOffsetDict.TryGetValue(key, out maxAllowToDeleteQueueOffset) && queueMessage.QueueOffset <= maxAllowToDeleteQueueOffset)
            //    {
            //        _messageDict.Remove(queueMessage.MessageOffset);
            //    }
            //}
        }
        private ILogRecord ReadMessage(BinaryReader reader)
        {
            var record = new MessageLogRecord();
            record.ReadFrom(reader);
            return record;
        }

        public void Dispose()
        {
            Shutdown();
        }
    }
}
