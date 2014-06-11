using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using ECommon.Components;
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
            _deleteMessageSQLFormat = "delete from [" + _setting.MessageTable + "] where Topic = '{0}' and QueueId = {1} and QueueOffset < {2}";
            _selectAllMessageSQL = "select * from [" + _setting.MessageTable + "] order by MessageOffset asc";
        }

        public void Recover()
        {
            RecoverAllMessages();
        }
        public void Start()
        {
            _persistMessageTaskId = _scheduleService.ScheduleTask("SqlServerMessageStore.PersistMessage", PersistMessage, _setting.CommitMessageInterval, _setting.CommitMessageInterval);
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
            _messageDict.Clear();
            using (var connection = new SqlConnection(_setting.ConnectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(_selectAllMessageSQL, connection))
                {
                    var maxMessageOffset = -1L;
                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        var messageOffset = (long)reader["MessageOffset"];
                        var topic = (string)reader["Topic"];
                        var queueId = (int)reader["QueueId"];
                        var queueOffset = (long)reader["QueueOffset"];
                        var body = (byte[])reader["Body"];
                        var storedTime = (DateTime)reader["StoredTime"];
                        _messageDict[messageOffset] = new QueueMessage(topic, body, messageOffset, queueId, queueOffset, storedTime);
                        maxMessageOffset = messageOffset;
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
                row["MessageOffset"] = message.MessageOffset;
                row["Topic"] = message.Topic;
                row["QueueId"] = message.QueueId;
                row["QueueOffset"] = message.QueueOffset;
                row["Body"] = message.Body;
                row["StoredTime"] = message.StoredTime;
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
                    sqlBulkCopy.ColumnMappings.Add("MessageOffset", "MessageOffset");
                    sqlBulkCopy.ColumnMappings.Add("Topic", "Topic");
                    sqlBulkCopy.ColumnMappings.Add("QueueId", "QueueId");
                    sqlBulkCopy.ColumnMappings.Add("QueueOffset", "QueueOffset");
                    sqlBulkCopy.ColumnMappings.Add("Body", "Body");
                    sqlBulkCopy.ColumnMappings.Add("StoredTime", "StoredTime");

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
            table.Columns.Add("MessageOffset", typeof(long));
            table.Columns.Add("Topic", typeof(string));
            table.Columns.Add("QueueId", typeof(int));
            table.Columns.Add("QueueOffset", typeof(long));
            table.Columns.Add("Body", typeof(byte[]));
            table.Columns.Add("StoredTime", typeof(DateTime));
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
