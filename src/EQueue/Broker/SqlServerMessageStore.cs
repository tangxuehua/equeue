using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Scheduling;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class SqlServerMessageStore : IMessageStore
    {
        private readonly ConcurrentDictionary<long, QueueMessage> _messageDict = new ConcurrentDictionary<long, QueueMessage>();
        private readonly ConcurrentDictionary<string, long> _queueAllowToDeleteOffsetDict = new ConcurrentDictionary<string, long>();
        private readonly ConcurrentDictionary<string, long> _queueMaxPersistedOffsetDict = new ConcurrentDictionary<string, long>();
        private readonly DataTable _messageDataTable;
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;
        private readonly SqlServerMessageStoreSetting _setting;
        private long _currentMessageOffset = -1;
        private long _persistedMessageOffset = -1;
        private int _persistMessageTaskId;
        private int _removeExceedMaxCacheMessageFromMemoryTaskId;
        private int _removeConsumedMessageFromMemoryTaskId;
        private int _deleteMessageTaskId;
        private int _isBatchPersistingMessages;
        private int _isRemovingConsumedMessages;

        private readonly string _deleteMessageSQLFormat;
        private readonly string _selectAllMessageSQL;
        private readonly string _batchLoadMessageSQLFormat;
        private readonly string _batchLoadQueueIndexSQLFormat;

        public bool SupportBatchLoadQueueIndex
        {
            get { return true; }
        }

        public SqlServerMessageStore(SqlServerMessageStoreSetting setting)
        {
            _setting = setting;
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _messageDataTable = BuildMessageDataTable();
            _deleteMessageSQLFormat = "delete from [" + _setting.MessageTable + "] where Topic = '{0}' and QueueId = {1} and QueueOffset < {2}";
            _selectAllMessageSQL = "select * from [" + _setting.MessageTable + "] order by MessageOffset asc";
            _batchLoadMessageSQLFormat = "select * from [" + _setting.MessageTable + "] where MessageOffset >= {0} and MessageOffset < {1}";
            _batchLoadQueueIndexSQLFormat = "select QueueOffset,MessageOffset from [" + _setting.MessageTable + "] where Topic = '{0}' and QueueId = {1} and QueueOffset >= {2} and QueueOffset < {3}";
        }

        public void Recover(Action<long, string, int, long> messageRecoveredCallback)
        {
            _logger.Info("Start to recover messages from db.");
            Clear();
            RecoverAllMessages(messageRecoveredCallback);
        }
        public void Start()
        {
            _persistMessageTaskId = _scheduleService.ScheduleTask("SqlServerMessageStore.TryPersistMessages", TryPersistMessages, _setting.PersistMessageInterval, _setting.PersistMessageInterval);
            _removeExceedMaxCacheMessageFromMemoryTaskId = _scheduleService.ScheduleTask("SqlServerMessageStore.RemoveExceedMaxCacheMessageFromMemory", RemoveExceedMaxCacheMessageFromMemory, _setting.RemoveExceedMaxCacheMessageFromMemoryInterval, _setting.RemoveExceedMaxCacheMessageFromMemoryInterval);
            _removeConsumedMessageFromMemoryTaskId = _scheduleService.ScheduleTask("SqlServerMessageStore.RemoveConsumedMessageFromMemory", RemoveConsumedMessageFromMemory, _setting.RemoveConsumedMessageFromMemoryInterval, _setting.RemoveConsumedMessageFromMemoryInterval);
            _deleteMessageTaskId = _scheduleService.ScheduleTask("SqlServerMessageStore.DeleteMessages", DeleteMessages, _setting.DeleteMessageInterval, _setting.DeleteMessageInterval);
        }
        public void Shutdown()
        {
            _scheduleService.ShutdownTask(_persistMessageTaskId);
            _scheduleService.ShutdownTask(_removeExceedMaxCacheMessageFromMemoryTaskId);
            _scheduleService.ShutdownTask(_removeConsumedMessageFromMemoryTaskId);
            _scheduleService.ShutdownTask(_deleteMessageTaskId);
        }
        public QueueMessage StoreMessage(int queueId, long queueOffset, Message message, string routingKey)
        {
            var nextOffset = GetNextOffset();
            var queueMessage = new QueueMessage(message.Topic, message.Code, message.Body, nextOffset, queueId, queueOffset, DateTime.Now, routingKey);
            _messageDict[nextOffset] = queueMessage;
            return queueMessage;
        }
        public QueueMessage GetMessage(long offset)
        {
            QueueMessage queueMessage;
            if (!_messageDict.TryGetValue(offset, out queueMessage))
            {
                BatchLoadMessage(offset, _setting.BatchLoadMessageSize);
            }
            if (_messageDict.TryGetValue(offset, out queueMessage))
            {
                return queueMessage;
            }
            return null;
        }
        public void UpdateMaxAllowToDeleteQueueOffset(string topic, int queueId, long queueOffset)
        {
            var key = string.Format("{0}-{1}", topic, queueId);
            _queueAllowToDeleteOffsetDict.AddOrUpdate(key, queueOffset, (currentKey, oldOffset) => queueOffset > oldOffset ? queueOffset : oldOffset);
        }
        public IDictionary<long, long> BatchLoadQueueIndex(string topic, int queueId, long startQueueOffset)
        {
            using (var connection = new SqlConnection(_setting.ConnectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(string.Format(_batchLoadQueueIndexSQLFormat, topic, queueId, startQueueOffset, startQueueOffset + _setting.BatchLoadQueueIndexSize), connection))
                {
                    var reader = command.ExecuteReader();
                    var dict = new Dictionary<long, long>();
                    while (reader.Read())
                    {
                        var queueOffset = (long)reader["QueueOffset"];
                        var messageOffset = (long)reader["MessageOffset"];
                        dict[queueOffset] = messageOffset;
                    }
                    return dict;
                }
            }
        }
        public IEnumerable<QueueMessage> QueryMessages(string topic, int? queueId, int? code, string routingKey, int pageIndex, int pageSize, out int total)
        {
            var whereSql = string.Empty;
            var hasCondition = false;
            if (!string.IsNullOrWhiteSpace(topic))
            {
                whereSql += string.Format(" where Topic = '{0}'", topic);
                hasCondition = true;
            }
            if (queueId != null)
            {
                var prefix = hasCondition ? " and " : " where ";
                whereSql += prefix + string.Format("QueueId = {0}", queueId.Value);
                if (!hasCondition)
                {
                    hasCondition = true;
                }
            }
            if (code != null)
            {
                var prefix = hasCondition ? " and " : " where ";
                whereSql += prefix + string.Format("Code = {0}", code.Value);
            }
            if (!string.IsNullOrWhiteSpace(routingKey))
            {
                var prefix = hasCondition ? " and " : " where ";
                whereSql += prefix + string.Format("RoutingKey = '{0}'", routingKey);
            }

            using (var connection = new SqlConnection(_setting.ConnectionString))
            {
                connection.Open();
                var countSql = string.Format(@"select count(1) from {0}{1}", _setting.MessageTable, whereSql);
                using (var command = new SqlCommand(countSql, connection))
                {
                    total = (int)command.ExecuteScalar();
                }

                var pageSql = string.Format(@"
                        SELECT * FROM (
                            SELECT ROW_NUMBER() OVER (ORDER BY m.MessageOffset desc) AS RowNumber,m.MessageOffset,m.Topic,m.QueueId,m.QueueOffset,m.Code,m.StoredTime,m.RoutingKey
                            FROM {0} m{1}) AS Total
                        WHERE RowNumber >= {2} AND RowNumber <= {3}",
                    _setting.MessageTable, whereSql, (pageIndex - 1) * pageSize + 1, pageIndex * pageSize);

                using (var command = new SqlCommand(pageSql, connection))
                {
                    var reader = command.ExecuteReader();
                    var messages = new List<QueueMessage>();
                    while (reader.Read())
                    {
                        var messageOffset = (long)reader["MessageOffset"];
                        var messageTopic = (string)reader["Topic"];
                        var messageQueueId = (int)reader["QueueId"];
                        var messageQueueOffset = (long)reader["QueueOffset"];
                        var messageCode = (int)reader["Code"];
                        var messageStoredTime = (DateTime)reader["StoredTime"];
                        var messageRoutingKey = (string)reader["RoutingKey"];
                        messages.Add(new QueueMessage(messageTopic, messageCode, null, messageOffset, messageQueueId, messageQueueOffset, messageStoredTime, messageRoutingKey));
                    }
                    return messages;
                }
            }
        }

        private void Clear()
        {
            _messageDict.Clear();
            _queueAllowToDeleteOffsetDict.Clear();
            _queueMaxPersistedOffsetDict.Clear();
            _messageDataTable.Rows.Clear();
            _currentMessageOffset = -1;
            _persistedMessageOffset = -1;
        }
        private void RemoveExceedMaxCacheMessageFromMemory()
        {
            var currentTotalCount = _messageDict.Count;
            var exceedCount = currentTotalCount - _setting.MessageMaxCacheSize;
            if (exceedCount > 0)
            {
                //First we should remove all the consumed messages from memory.
                RemoveConsumedMessageFromMemory();

                var currentMessageOffset = _currentMessageOffset;
                var currentPersistedMessageOffet = _persistedMessageOffset;
                currentTotalCount = _messageDict.Count;
                exceedCount = currentTotalCount - _setting.MessageMaxCacheSize;
                if (exceedCount <= 0)
                {
                    return;
                }

                //If the remaining message count still exceed the max message cache size, then we try to remove all the exceeded unconsumed and persisted messages.
                var totalRemovedCount = 0;
                var offset = currentPersistedMessageOffet;
                while (totalRemovedCount < exceedCount && offset >= 0)
                {
                    QueueMessage removedMessage;
                    if (_messageDict.TryRemove(offset, out removedMessage))
                    {
                        totalRemovedCount++;
                    }
                    offset--;
                }
                if (totalRemovedCount > 0)
                {
                    _logger.InfoFormat("Auto removed {0} unconsumed messages which exceed the max cache size, current total unconsumed message count:{1}, current exceed count:{2}", totalRemovedCount, currentTotalCount, exceedCount);
                }
                else
                {
                    _logger.ErrorFormat("The current unconsumed message count in memory exceeds the message max cache size, but we cannot remove any messages from memory, please check if all the messages are persisted. exceed count:{0}, current total unconsumed message count:{1}, currentMessageOffset:{2}, currentPersistedMessageOffset:{3}",
                        exceedCount, currentTotalCount, currentMessageOffset, currentPersistedMessageOffet);
                }
            }
        }
        private void RemoveConsumedMessageFromMemory()
        {
            if (Interlocked.CompareExchange(ref _isRemovingConsumedMessages, 1, 0) == 0)
            {
                try
                {
                    var totalRemovedCount = 0;
                    foreach (var queueMessage in _messageDict.Values)
                    {
                        var key = string.Format("{0}-{1}", queueMessage.Topic, queueMessage.QueueId);
                        long maxAllowToDeleteQueueOffset;
                        if (_queueAllowToDeleteOffsetDict.TryGetValue(key, out maxAllowToDeleteQueueOffset) && queueMessage.QueueOffset <= maxAllowToDeleteQueueOffset)
                        {
                            QueueMessage removedMessage;
                            if (_messageDict.TryRemove(queueMessage.MessageOffset, out removedMessage))
                            {
                                totalRemovedCount++;
                            }
                        }
                    }
                    if (totalRemovedCount > 0)
                    {
                        _logger.InfoFormat("Auto removed {0} consumed messages from memory.", totalRemovedCount);
                    }
                }
                catch (Exception ex)
                {
                    _logger.Error("Failed to remove consumed messages.", ex);
                }
                finally
                {
                    Interlocked.Exchange(ref _isRemovingConsumedMessages, 0);
                }
            }
        }
        private void RecoverAllMessages(Action<long, string, int, long> messageRecoveredCallback)
        {
            using (var connection = new SqlConnection(_setting.ConnectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(_selectAllMessageSQL, connection))
                {
                    var maxMessageOffset = -1L;
                    var count = 0;
                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        var queueMessage = PopulateMessageFromReader(reader);
                        if (count < _setting.MessageMaxCacheSize)
                        {
                            _messageDict[queueMessage.MessageOffset] = queueMessage;
                        }
                        messageRecoveredCallback(queueMessage.MessageOffset, queueMessage.Topic, queueMessage.QueueId, queueMessage.QueueOffset);
                        _queueMaxPersistedOffsetDict[string.Format("{0}-{1}", queueMessage.Topic, queueMessage.QueueId)] = queueMessage.QueueOffset;
                        maxMessageOffset = queueMessage.MessageOffset;
                        count++;
                    }
                    if (maxMessageOffset >= 0)
                    {
                        _currentMessageOffset = maxMessageOffset;
                        _persistedMessageOffset = maxMessageOffset;
                    }
                    _logger.InfoFormat("{0} messages recovered, current message offset:{1}", count, _currentMessageOffset);
                }
            }
        }
        private void BatchLoadMessage(long startOffset, int batchSize)
        {
            using (var connection = new SqlConnection(_setting.ConnectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(string.Format(_batchLoadMessageSQLFormat, startOffset, startOffset + batchSize), connection))
                {
                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        var queueMessage = PopulateMessageFromReader(reader);
                        _messageDict[queueMessage.MessageOffset] = queueMessage;
                    }
                }
            }
        }
        private QueueMessage PopulateMessageFromReader(IDataReader reader)
        {
            var messageOffset = (long)reader["MessageOffset"];
            var topic = (string)reader["Topic"];
            var queueId = (int)reader["QueueId"];
            var queueOffset = (long)reader["QueueOffset"];
            var code = (int)reader["Code"];
            var body = (byte[])reader["Body"];
            var storedTime = (DateTime)reader["StoredTime"];
            var routingKey = (string)reader["RoutingKey"];
            return new QueueMessage(topic, code, body, messageOffset, queueId, queueOffset, storedTime, routingKey);
        }
        private void TryPersistMessages()
        {
            if (Interlocked.CompareExchange(ref _isBatchPersistingMessages, 1, 0) == 0)
            {
                try
                {
                    var hasPersistedAllMessages = PersistMessages();
                    while (!hasPersistedAllMessages)
                    {
                        hasPersistedAllMessages = PersistMessages();
                    }
                }
                catch (Exception ex)
                {
                    _logger.Error(string.Format("Failed to persist messages to db, last persisted offset:{0}", _persistedMessageOffset), ex);
                }
                finally
                {
                    Interlocked.Exchange(ref _isBatchPersistingMessages, 0);
                }
            }
        }
        private bool PersistMessages()
        {
            var messages = new List<QueueMessage>();
            var currentOffset = _persistedMessageOffset + 1;

            while (currentOffset <= _currentMessageOffset && messages.Count < _setting.PersistMessageMaxCount)
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
                return true;
            }

            var queueMaxPersistedOffsetDict = new ConcurrentDictionary<string, long>();
            _messageDataTable.Rows.Clear();
            foreach (var message in messages)
            {
                var row = _messageDataTable.NewRow();
                row["MessageOffset"] = message.MessageOffset;
                row["Topic"] = message.Topic;
                row["QueueId"] = message.QueueId;
                row["QueueOffset"] = message.QueueOffset;
                row["Code"] = message.Code;
                row["Body"] = message.Body;
                row["StoredTime"] = message.StoredTime;
                row["RoutingKey"] = message.RoutingKey;
                _messageDataTable.Rows.Add(row);
                queueMaxPersistedOffsetDict[string.Format("{0}-{1}", message.Topic, message.QueueId)] = message.QueueOffset;
            }

            var maxMessageOffset = messages.Last().MessageOffset;
            if (BatchPersistMessages(_messageDataTable, maxMessageOffset))
            {
                _persistedMessageOffset = maxMessageOffset;
                foreach (var entry in queueMaxPersistedOffsetDict)
                {
                    _queueMaxPersistedOffsetDict[entry.Key] = entry.Value;
                }
            }

            var hasFetchedAllEvents = messages.Count < _setting.PersistMessageMaxCount;
            return hasFetchedAllEvents;
        }
        private bool BatchPersistMessages(DataTable messageDataTable, long maxMessageOffset)
        {
            using (var connection = new SqlConnection(_setting.ConnectionString))
            {
                connection.Open();

                var transaction = connection.BeginTransaction();
                var result = true;

                using (var copy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                {
                    copy.BatchSize = _setting.BulkCopyBatchSize;
                    copy.BulkCopyTimeout = _setting.BulkCopyTimeout;
                    copy.DestinationTableName = _setting.MessageTable;
                    copy.ColumnMappings.Add("MessageOffset", "MessageOffset");
                    copy.ColumnMappings.Add("Topic", "Topic");
                    copy.ColumnMappings.Add("QueueId", "QueueId");
                    copy.ColumnMappings.Add("QueueOffset", "QueueOffset");
                    copy.ColumnMappings.Add("Code", "Code");
                    copy.ColumnMappings.Add("Body", "Body");
                    copy.ColumnMappings.Add("StoredTime", "StoredTime");
                    copy.ColumnMappings.Add("RoutingKey", "RoutingKey");

                    try
                    {
                        copy.WriteToServer(messageDataTable);
                        transaction.Commit();
                        _logger.DebugFormat("Success to bulk copy {0} messages to db, maxMessageOffset:{1}", messageDataTable.Rows.Count, maxMessageOffset);
                    }
                    catch (Exception ex)
                    {
                        result = false;
                        transaction.Rollback();
                        _logger.Error(string.Format("Failed to bulk copy {0} messages to db, maxMessageOffset:{1}", messageDataTable.Rows.Count, maxMessageOffset), ex);
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
            table.Columns.Add("Code", typeof(int));
            table.Columns.Add("Body", typeof(byte[]));
            table.Columns.Add("StoredTime", typeof(DateTime));
            table.Columns.Add("RoutingKey", typeof(string));
            return table;
        }
        private void DeleteMessages()
        {
            if (!IsTimeToDelete())
            {
                return;
            }

            foreach (var entry in _queueAllowToDeleteOffsetDict)
            {
                if (entry.Value <= 0) continue;

                long maxPersistedOffset;
                if (!_queueMaxPersistedOffsetDict.TryGetValue(entry.Key, out maxPersistedOffset))
                {
                    _logger.ErrorFormat("Failed to delete message of queue as the max persisted offset of the queue not exist. queueKey:{0}, allowToDeleteOffset:{1}", entry.Key, entry.Value);
                    return;
                }
                var items = entry.Key.Split(new string[] { "-" }, StringSplitOptions.None);
                var topic = items[0];
                var queueId = int.Parse(items[1]);
                var maxAllowToDeleteQueueOffset = entry.Value < maxPersistedOffset ? entry.Value : maxPersistedOffset;
                DeleteMessages(topic, queueId, maxAllowToDeleteQueueOffset);
            }
        }
        private void DeleteMessages(string topic, int queueId, long maxAllowToDeleteQueueOffset)
        {
            using (var connection = new SqlConnection(_setting.ConnectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(string.Format(_deleteMessageSQLFormat, topic, queueId, maxAllowToDeleteQueueOffset), connection))
                {
                    var deletedMessageCount = command.ExecuteNonQuery();
                    if (deletedMessageCount > 0)
                    {
                        _logger.DebugFormat("Deleted {0} messages, topic={1}, queueId={2}, queueOffset<{3}.", deletedMessageCount, topic, queueId, maxAllowToDeleteQueueOffset);
                    }
                }
            }
        }
        private long GetNextOffset()
        {
            return Interlocked.Increment(ref _currentMessageOffset);
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
