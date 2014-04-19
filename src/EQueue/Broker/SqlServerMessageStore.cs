using System;
using System.Collections.Concurrent;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using ECommon.IoC;
using ECommon.Logging;
using ECommon.Scheduling;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class SqlServerMessageStore : IMessageStore
    {
        private readonly BlockingCollection<QueueMessage> _messageBufferQueue = new BlockingCollection<QueueMessage>();
        private readonly ConcurrentDictionary<long, QueueMessage> _messageCacheDict = new ConcurrentDictionary<long, QueueMessage>();
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;
        private long _currentOffset = -1;
        private int _persistMessageTaskId;
        private int _deleteMessageTaskId;
        private SqlServerMessageStoreSetting _setting;
        private DataTable _messageDataTable;
        private string _deleteMessageSQLFormat;

        public SqlServerMessageStore(SqlServerMessageStoreSetting setting)
        {
            _setting = setting;
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _messageDataTable = BuildMessageDataTable();
            _deleteMessageSQLFormat = "delete from [" + _setting.MessageTable + "] where stored_time < '{0}'";
        }

        public void Start()
        {
            _persistMessageTaskId = _scheduleService.ScheduleTask(PersistMessage, _setting.CommitMessageInterval, _setting.CommitMessageInterval);
            _deleteMessageTaskId = _scheduleService.ScheduleTask(DeleteMessage, _setting.DeleteMessageInterval, _setting.DeleteMessageInterval);
        }
        public void Shutdown()
        {
            _scheduleService.ShutdownTask(_persistMessageTaskId);
            _scheduleService.ShutdownTask(_deleteMessageTaskId);
        }
        public QueueMessage StoreMessage(int queueId, long queueOffset, Message message)
        {
            var nextOffset = GetNextOffset();
            var queueMessage = new QueueMessage(message.Topic, message.Body, nextOffset, queueId, queueOffset, DateTime.Now);
            _messageBufferQueue.Add(queueMessage);
            _messageCacheDict[nextOffset] = queueMessage;
            return queueMessage;
        }
        public QueueMessage GetMessage(long offset)
        {
            QueueMessage queueMessage;
            if (_messageCacheDict.TryGetValue(offset, out queueMessage))
            {
                return queueMessage;
            }
            return null;
        }
        public bool RemoveMessage(long offset)
        {
            QueueMessage queueMessage;
            return _messageCacheDict.TryRemove(offset, out queueMessage);
        }

        private void PersistMessage()
        {
            _messageDataTable.Rows.Clear();
            var bufferQueueMessageCount = _messageBufferQueue.Count;
            var fetchCount = bufferQueueMessageCount < _setting.MessageCommitMaxCount ? bufferQueueMessageCount : _setting.MessageCommitMaxCount;

            for (var index = 0; index < fetchCount; index++)
            {
                var message = _messageBufferQueue.Take();
                var row = _messageDataTable.NewRow();
                row["id"] = message.MessageOffset;
                row["topic"] = message.Topic;
                row["queueId"] = message.QueueId;
                row["queueOffset"] = message.QueueOffset;
                row["body"] = message.Body;
                row["storedTime"] = message.StoredTime;
                _messageDataTable.Rows.Add(row);
            }

            BatchCommitMessages(_messageDataTable);
        }
        private void BatchCommitMessages(DataTable messageDataTable)
        {
            using (var connection = new SqlConnection(_setting.ConnectionString))
            {
                connection.Open();
                using (var sqlBulkCopy = new SqlBulkCopy(connection))
                {
                    sqlBulkCopy.BatchSize = 10000;
                    sqlBulkCopy.BulkCopyTimeout = 60;
                    sqlBulkCopy.DestinationTableName = _setting.MessageTable;
                    sqlBulkCopy.ColumnMappings.Add("id", "id");
                    sqlBulkCopy.ColumnMappings.Add("topic", "topic");
                    sqlBulkCopy.ColumnMappings.Add("queueId", "queue_id");
                    sqlBulkCopy.ColumnMappings.Add("queueOffset", "queue_offset");
                    sqlBulkCopy.ColumnMappings.Add("body", "body");
                    sqlBulkCopy.ColumnMappings.Add("storedTime", "stored_time");

                    try
                    {
                        sqlBulkCopy.WriteToServer(messageDataTable);
                    }
                    catch (Exception ex)
                    {
                        //TODO, retry logic for design.
                        _logger.Error("Exception raised when bulk copy messages.", ex);
                    }
                }
            }
        }
        private void DeleteMessage()
        {
            if (!IsTimeToDelete())
            {
                return;
            }
            var current = DateTime.Now;
            var deleteMessageMaxTime = current;
            var storedTimeOfCurrentExistingMinMessage = GetStoredTimeOfCurrentExistingMinMessage();
            if (storedTimeOfCurrentExistingMinMessage != null && storedTimeOfCurrentExistingMinMessage.Value < current)
            {
                deleteMessageMaxTime = storedTimeOfCurrentExistingMinMessage.Value;
            }
            DeleteMessageBeforeTime(deleteMessageMaxTime);
        }
        private DataTable BuildMessageDataTable()
        {
            var table = new DataTable();
            table.Columns.Add("id", typeof(long));
            table.Columns.Add("topic", typeof(string));
            table.Columns.Add("queueId", typeof(int));
            table.Columns.Add("queueOffset", typeof(long));
            table.Columns.Add("body", typeof(byte[]));
            table.Columns.Add("storedTime", typeof(DateTime));
            return table;
        }
        private long GetNextOffset()
        {
            return Interlocked.Increment(ref _currentOffset);
        }
        private void DeleteMessageBeforeTime(DateTime deleteMessageMaxTime)
        {
            var sql = string.Format(_deleteMessageSQLFormat, deleteMessageMaxTime);
            using (var connection = new SqlConnection(_setting.ConnectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(sql, connection))
                {
                    var deletedMessageCount = command.ExecuteNonQuery();
                    if (deletedMessageCount > 0)
                    {
                        _logger.DebugFormat("Deleted {0} message from sql server, delete message max time:{1}", deletedMessageCount, deleteMessageMaxTime);
                    }
                }
            }
        }
        private DateTime? GetStoredTimeOfCurrentExistingMinMessage()
        {
            var currentExistingMinOffset = GetCurrentExistingMinOffset();
            if (currentExistingMinOffset != null)
            {
                var currentExistingMinMessage = GetMessage(currentExistingMinOffset.Value);
                if (currentExistingMinMessage != null)
                {
                    return currentExistingMinMessage.StoredTime;
                }
            }
            return null;
        }
        private long? GetCurrentExistingMinOffset()
        {
            long? minOffset = null;
            foreach (var key in _messageCacheDict.Keys)
            {
                if (minOffset == null)
                {
                    minOffset = key;
                }
                else if (key < minOffset.Value)
                {
                    minOffset = key;
                }
            }
            return minOffset;
        }
        private bool IsTimeToDelete()
        {
            return DateTime.Now.Hour == _setting.DeleteMessageHourOfDay;
        }
    }
}
