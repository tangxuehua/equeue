using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Utilities;
using EQueue.Broker.Storage;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class FileMessageStore : IMessageStore, IDisposable
    {
        private readonly TFChunkWriter _chunkWriter;
        private readonly TFChunkReader _chunkReader;
        private readonly TFChunkManager _chunkManager;
        private readonly ConcurrentDictionary<string, long> _queueConsumedOffsetDict = new ConcurrentDictionary<string, long>();
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;
        private long _currentOffset = -1;

        public long CurrentMessageOffset
        {
            get { return _currentOffset; }
        }
        public long PersistedMessageOffset
        {
            get { return _currentOffset; }
        }
        public bool SupportBatchLoadQueueIndex
        {
            get { return false; }
        }

        public FileMessageStore(TFChunkManager chunkManager)
        {
            _chunkManager = chunkManager;
            _chunkWriter = new TFChunkWriter(chunkManager);
            _chunkReader = new TFChunkReader(chunkManager, _chunkWriter, 0);
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void Recover(IEnumerable<QueueConsumedOffset> queueConsumedOffsets, Action<long, string, int, long> messageRecoveredCallback)
        {
            _chunkManager.Load();
            _chunkWriter.Open();
        }
        public void Start()
        {
            _scheduleService.StartTask("FileMessageStore.RemoveConsumedMessagesFromMemory", RemoveConsumedMessagesFromMemory, 5000, 5000);
        }
        public void Shutdown()
        {
            _scheduleService.StopTask("FileMessageStore.RemoveConsumedMessagesFromMemory");
            _chunkWriter.Close();
            _chunkManager.Close();
        }
        public long GetNextMessageOffset()
        {
            return Interlocked.Increment(ref _currentOffset);
        }
        public MessageStoreResult StoreMessage(int queueId, long messageOffset, long queueOffset, Message message, string routingKey)
        {
            var logPosition = _chunkWriter.CurrentChunk.GlobalDataPosition;
            var record = new MessageLogRecord(
                1,
                logPosition,
                message.Topic,
                message.Code,
                ObjectId.GenerateNewStringId(),
                message.Key,
                message.Body,
                messageOffset,
                queueId,
                queueOffset,
                routingKey,
                message.CreatedTime,
                DateTime.Now);
            _chunkWriter.Write(record);

            return new MessageStoreResult(record);
        }
        public MessageLogRecord GetMessage(long logPosition)
        {
            var result = _chunkReader.TryReadAt(logPosition);
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
        public IDictionary<long, long> BatchLoadQueueIndex(string topic, int queueId, long startQueueOffset)
        {
            throw new NotImplementedException();
        }
        public IEnumerable<QueueMessage> QueryMessages(string topic, int? queueId, int? code, string routingKey, int pageIndex, int pageSize, out int total)
        {
            total = 0;
            return new QueueMessage[0];
            //TODO
            //var source = _messageDict.Values;
            //var predicate = new Func<QueueMessage, bool>(x =>
            //{
            //    var pass = true;
            //    if (!string.IsNullOrEmpty(topic))
            //    {
            //        pass = x.Topic == topic;
            //    }
            //    if (pass && queueId != null)
            //    {
            //        pass = x.QueueId == queueId.Value;
            //    }
            //    if (pass && code != null)
            //    {
            //        pass = x.Code == code.Value;
            //    }
            //    if (pass && !string.IsNullOrEmpty(routingKey))
            //    {
            //        pass = x.RoutingKey == routingKey;
            //    }
            //    return pass;
            //});

            //total = source.Count(predicate);
            //return source.Where(predicate).Skip((pageIndex - 1) * pageSize).Take(pageSize);
        }

        private void RemoveConsumedMessagesFromMemory()
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

        public void Dispose()
        {
            Shutdown();
        }
    }
}
