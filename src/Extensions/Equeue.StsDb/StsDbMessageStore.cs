using System;
using System.Collections.Concurrent;
using System.Threading;
using ECommon.IoC;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Serializing;
using EQueue.Broker;
using EQueue.Protocols;
using STSdb4.Database;

namespace Equeue.StsDb
{
    public class StsDbMessageStore : IMessageStore
    {
        private IBinarySerializer _binarySerializer;
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;
        private readonly object _lockObj = new object();
        private string _storageFileName;
        private const string QueueMessageTableName = "QueueMessageTable";
        private IStorageEngine _storageEngine;
        private ITable<long, byte[]> _queueMessageTable;
        private long _currentOffset = -1;
        private long _originalOffset = -1;
        private int _commitQueueMessageTaskId;
        private BlockingCollection<MessageInfo> _localQueue;
        private Worker _persistMessageWorker;

        public Action<QueueMessage> MessageStoredCallback { get; set; }

        public StsDbMessageStore(string storageFileName)
        {
            _storageFileName = storageFileName;
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _localQueue = new BlockingCollection<MessageInfo>(new ConcurrentQueue<MessageInfo>());
            _persistMessageWorker = new Worker(PersistQueueMessage);
        }

        public void Start()
        {
            _storageEngine = STSdb.FromFile(_storageFileName);
            _queueMessageTable = _storageEngine.OpenXTable<long, byte[]>(QueueMessageTableName);
            if (_queueMessageTable.Count() > 0)
            {
                _currentOffset = _queueMessageTable.LastRow.Key;
                _originalOffset = _currentOffset;
            }
            _persistMessageWorker.Start();
            _commitQueueMessageTaskId = _scheduleService.ScheduleTask(CommitQueueMessage, 1000, 1000);
            _logger.InfoFormat("Started, max message offset:{0}", _currentOffset);
        }
        public void Shutdown()
        {
            while (_localQueue.Count > 0)
            {
                Thread.Sleep(100);
            }
            _persistMessageWorker.Stop();
            _storageEngine.Commit();
            _storageEngine.Close();
            _storageEngine.Dispose();
            _scheduleService.ShutdownTask(_commitQueueMessageTaskId);
            _logger.InfoFormat("Shutdown, max message offset:{0}", _currentOffset);
        }
        public long StoreMessage(MessageInfo messageInfo)
        {
            var offset = GetNextOffset();
            var queueMessage = new QueueMessage(
                messageInfo.Message.Topic,
                messageInfo.Message.Body,
                offset,
                messageInfo.Queue.QueueId,
                messageInfo.QueueOffset, DateTime.Now);
            messageInfo.QueueMessage = queueMessage;
            _localQueue.Add(messageInfo);
            return offset;
        }
        public QueueMessage GetMessage(long offset)
        {
            byte[] data;
            if (_queueMessageTable.TryGet(offset, out data))
            {
                return _binarySerializer.Deserialize<QueueMessage>(data);
            }
            return null;
        }
        public bool RemoveMessage(long offset)
        {
            _queueMessageTable.Delete(offset);
            return true;
        }

        private long GetNextOffset()
        {
            return Interlocked.Increment(ref _currentOffset);
        }
        long _totalPersistedMessageCount = 0;
        private void PersistQueueMessage()
        {
            var messageInfo = _localQueue.Take();
            var data = _binarySerializer.Serialize(messageInfo.QueueMessage);
            _queueMessageTable[messageInfo.QueueMessage.MessageOffset] = data;
            _totalPersistedMessageCount++;
            if (_totalPersistedMessageCount % 1000 == 0)
            {
                _logger.InfoFormat("Total persisted message:{0}, original offset when broker start:{1}", _totalPersistedMessageCount, _originalOffset);
            }
            messageInfo.Queue.SetQueueMessage(messageInfo.QueueMessage.QueueOffset, messageInfo.QueueMessage);
        }
        private void CommitQueueMessage()
        {
            _storageEngine.Commit();
        }
    }
}
