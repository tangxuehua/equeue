using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Scheduling;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class SqlServerMessageStore : IMessageStore
    {
        private readonly ConcurrentDictionary<long, QueueMessage> _messageDict = new ConcurrentDictionary<long, QueueMessage>();
        private readonly ConcurrentDictionary<string, OffsetState> _queueOffsetDict = new ConcurrentDictionary<string, OffsetState>();
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;
        private readonly SqlServerMessageStoreSetting _setting;
        private readonly DataTable _messageDataTable;
        private long _currentOffset = -1;
        private long _persistedOffset = -1;
        private int _persistMessageTaskId;
        private int _deleteMessageTaskId;

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
            _persistMessageTaskId = _scheduleService.ScheduleTask("SqlServerMessageStore.PersistMessages", PersistMessages, _setting.PersistMessageInterval, _setting.PersistMessageInterval);
            _deleteMessageTaskId = _scheduleService.ScheduleTask("SqlServerMessageStore.DeleteMessages", DeleteMessages, _setting.DeleteMessageInterval, _setting.DeleteMessageInterval);
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
        public void UpdateMaxAllowToDeleteMessageOffset(string topic, int queueId, long messageOffset)
        {
            var key = string.Format("{0}-{1}", topic, queueId);
            _queueOffsetDict.AddOrUpdate(key, new OffsetState { MaxAllowToDeleteOffset = messageOffset }, (currentKey, state) =>
            {
                if (messageOffset > state.MaxAllowToDeleteOffset)
                {
                    state.MaxAllowToDeleteOffset = messageOffset;
                }
                return state;
            });
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
                        _persistedOffset = maxMessageOffset;
                    }
                    _logger.InfoFormat("{0} messages recovered, current message offset:{1}", _messageDict.Count, _currentOffset);
                }
            }
        }
        private void PersistMessages()
        {
            var messages = new List<QueueMessage>();
            var currentOffset = _persistedOffset + 1;

            while (currentOffset <= _currentOffset && messages.Count < _setting.PersistMessageMaxCount)
            {
                QueueMessage message;
                if (_messageDict.TryGetValue(currentOffset, out message))
                {
                    messages.Add(message);
                }
                currentOffset++;
            }

            if (messages.Count == 0)
            {
                return;
            }

            _messageDataTable.Rows.Clear();
            foreach (var message in messages)
            {
                var row = _messageDataTable.NewRow();
                row["MessageOffset"] = message.MessageOffset;
                row["Topic"] = message.Topic;
                row["QueueId"] = message.QueueId;
                row["QueueOffset"] = message.QueueOffset;
                row["Body"] = message.Body;
                row["StoredTime"] = message.StoredTime;
                _messageDataTable.Rows.Add(row);
            }

            if (BatchPersistMessages(_messageDataTable))
            {
                _persistedOffset = messages.Last().MessageOffset;
            }
        }
        private bool BatchPersistMessages(DataTable messageDataTable)
        {
            using (var connection = new SqlConnection(_setting.ConnectionString))
            {
                connection.Open();

                var transaction = connection.BeginTransaction();
                var result = true;

                using (var copy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                {
                    copy.BatchSize = 10000;
                    copy.BulkCopyTimeout = 60;
                    copy.DestinationTableName = _setting.MessageTable;
                    copy.ColumnMappings.Add("MessageOffset", "MessageOffset");
                    copy.ColumnMappings.Add("Topic", "Topic");
                    copy.ColumnMappings.Add("QueueId", "QueueId");
                    copy.ColumnMappings.Add("QueueOffset", "QueueOffset");
                    copy.ColumnMappings.Add("Body", "Body");
                    copy.ColumnMappings.Add("StoredTime", "StoredTime");

                    try
                    {
                        copy.WriteToServer(messageDataTable);
                        transaction.Commit();
                        _logger.DebugFormat("Success to bulk copy {0} messages to db.", messageDataTable.Rows.Count);
                    }
                    catch (Exception ex)
                    {
                        result = false;
                        transaction.Rollback();
                        _logger.Error("Failed to bulk copy messages to db.", ex);
                    }
                }

                return result;
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
        private void DeleteMessages()
        {
            if (!IsTimeToDelete())
            {
                return;
            }

            foreach (var entry in _queueOffsetDict)
            {
                var items = entry.Key.Split(new string[] { "-" }, StringSplitOptions.None);
                var topic = items[0];
                var queueId = int.Parse(items[1]);
                var offsetState = entry.Value;
                DeleteMessages(topic, queueId, offsetState);
            }
        }
        private void DeleteMessages(string topic, int queueId, OffsetState offsetState)
        {
            if (offsetState.MaxDeletedOffset >= offsetState.MaxAllowToDeleteOffset)
            {
                return;
            }

            var maxAllowToDeleteOffset = offsetState.MaxAllowToDeleteOffset;
            using (var connection = new SqlConnection(_setting.ConnectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(string.Format(_deleteMessageSQLFormat, topic, queueId, maxAllowToDeleteOffset), connection))
                {
                    var deletedMessageCount = command.ExecuteNonQuery();
                    if (deletedMessageCount > 0)
                    {
                        _logger.DebugFormat("Deleted {0} messages, topic={1}, queueId={2}, queueOffset<{3}.", deletedMessageCount, topic, queueId, maxAllowToDeleteOffset);
                    }
                }
            }
            offsetState.MaxDeletedOffset = maxAllowToDeleteOffset;
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

        class OffsetState
        {
            public long MaxAllowToDeleteOffset;
            public long MaxDeletedOffset;
        }
    }
}
