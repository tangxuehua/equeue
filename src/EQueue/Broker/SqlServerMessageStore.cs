using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
        private readonly BlockingCollection<QueueMessage> _messageQueue = new BlockingCollection<QueueMessage>();
        private readonly ConcurrentDictionary<long, QueueMessage> _messageDict = new ConcurrentDictionary<long, QueueMessage>();
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;
        private long _currentOffset = -1;
        private int _persistMessageTaskId;
        private SqlServerMessageStoreSetting _setting;
        private DataTable _messageDataTable;
        private string _deleteMessageSQLFormat;
        private string _selectAllMessageSQL;

        public IEnumerable<QueueMessage> Messages { get { return _messageDict.Values; } }

        public SqlServerMessageStore(SqlServerMessageStoreSetting setting)
        {
            _setting = setting;
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _messageDataTable = BuildMessageDataTable();
            _deleteMessageSQLFormat = "delete from [" + _setting.MessageTable + "] where topic = '{0}' and queue_id = {1} and queue_offset < {2}";
            _selectAllMessageSQL = "select * from [" + _setting.MessageTable + "] order by id asc";
        }

        public void Recover()
        {
            RecoverAllMessages();
        }
        public void Start()
        {
            _persistMessageTaskId = _scheduleService.ScheduleTask(PersistMessage, _setting.CommitMessageInterval, _setting.CommitMessageInterval);
        }
        public void Shutdown()
        {
            _scheduleService.ShutdownTask(_persistMessageTaskId);
        }
        public QueueMessage StoreMessage(int queueId, long queueOffset, Message message)
        {
            var nextOffset = GetNextOffset();
            var queueMessage = new QueueMessage(message.Topic, message.Body, nextOffset, queueId, queueOffset, DateTime.Now);
            _messageQueue.Add(queueMessage);
            _messageDict[nextOffset] = queueMessage;
            return queueMessage;
        }
        public QueueMessage GetMessage(long offset)
        {
            QueueMessage queueMessage;
            if (_messageDict.TryGetValue(offset, out queueMessage))
            {
                return queueMessage;
            }
            return null;
        }
        public void RemoveMessage(long messageOffset)
        {
            QueueMessage queueMessage;
            _messageDict.TryRemove(messageOffset, out queueMessage);
        }
        private void RecoverAllMessages()
        {
            using (var connection = new SqlConnection(_setting.ConnectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(_selectAllMessageSQL, connection))
                {
                    var maxMessageOffset = -1L;
                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        var id = (long)reader["id"];
                        var topic = (string)reader["topic"];
                        var queueId = (int)reader["queue_id"];
                        var queueOffset = (long)reader["queue_offset"];
                        var body = (byte[])reader["body"];
                        var storedTime = (DateTime)reader["stored_time"];
                        _messageDict[id] = new QueueMessage(topic, body, id, queueId, queueOffset, storedTime);
                        maxMessageOffset = id;
                    }
                    if (maxMessageOffset >= 0)
                    {
                        _currentOffset = maxMessageOffset;
                    }
                    _logger.InfoFormat("{0} messages recovered, current message offset:{1}", _messageDict.Count, _currentOffset);
                }
            }
        }
        private void PersistMessage()
        {
            _messageDataTable.Rows.Clear();
            var bufferQueueMessageCount = _messageQueue.Count;
            var fetchCount = bufferQueueMessageCount < _setting.MessageCommitMaxCount ? bufferQueueMessageCount : _setting.MessageCommitMaxCount;
            if (fetchCount == 0)
            {
                return;
            }

            for (var index = 0; index < fetchCount; index++)
            {
                var message = _messageQueue.Take();
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
                        _logger.DebugFormat("Bulk copied {0} message to db.", messageDataTable.Rows.Count);
                    }
                    catch (Exception ex)
                    {
                        //TODO, retry logic for design.
                        _logger.Error("Exception raised when bulk copy messages.", ex);
                    }
                }
            }
        }
        public void DeleteMessages(string topic, int queueId, long maxQueueOffset)
        {
            if (!IsTimeToDelete())
            {
                return;
            }

            using (var connection = new SqlConnection(_setting.ConnectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(string.Format(_deleteMessageSQLFormat, topic, queueId, maxQueueOffset), connection))
                {
                    var deletedMessageCount = command.ExecuteNonQuery();
                    if (deletedMessageCount > 0)
                    {
                        _logger.DebugFormat("Deleted {0} messages, topic={1}, queueId={2}, queueOffset<{3}.", deletedMessageCount, topic, queueId, maxQueueOffset);
                    }
                }
            }
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
        private bool IsTimeToDelete()
        {
            if (_setting.DeleteMessageHourOfDay == -1)
            {
                return true;
            }
            return DateTime.Now.Hour == _setting.DeleteMessageHourOfDay;
        }
    }
}
