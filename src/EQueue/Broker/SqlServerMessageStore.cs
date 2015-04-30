using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using ECommon.Components;
using ECommon.Dapper;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Serializing;
using ECommon.Utilities;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class SqlServerMessageStore : IMessageStore
    {
        private const string MessageFileName = "Messages.txt";
        private readonly byte[] EmptyBody = new byte[0];
        private readonly ConcurrentDictionary<long, QueueMessage> _messageDict = new ConcurrentDictionary<long, QueueMessage>();
        private readonly ConcurrentDictionary<string, long> _queueConsumedOffsetDict = new ConcurrentDictionary<string, long>();
        private readonly ConcurrentDictionary<string, long> _queueMaxPersistedOffsetDict = new ConcurrentDictionary<string, long>();
        private readonly DataTable _messageDataTable;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;
        private readonly ILogger _messageLogger;
        private readonly SqlServerMessageStoreSetting _setting;
        private long _currentMessageOffset = -1;
        private long _persistedMessageOffset = -1;
        private int _isBatchPersistingMessages;
        private int _isRemovingConsumedMessages;
        private readonly IList<int> _taskIds;

        private readonly string _deleteMessagesByTimeAndMaxQueueOffsetFormat;
        private readonly string _selectMaxMessageOffsetSQL;
        private readonly string _selectAllMessageSQL;
        private readonly string _getMessageOffsetByQueueOffset;
        private readonly string _batchLoadMessageSQLFormat;
        private readonly string _batchLoadQueueIndexSQLFormat;

        public long CurrentMessageOffset
        {
            get { return _currentMessageOffset; }
        }
        public long PersistedMessageOffset
        {
            get
            {
                return _persistedMessageOffset;
            }
        }
        public bool SupportBatchLoadQueueIndex
        {
            get { return true; }
        }

        public SqlServerMessageStore(SqlServerMessageStoreSetting setting)
        {
            _setting = setting;
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _messageLogger = ObjectContainer.Resolve<ILoggerFactory>().Create("MessageLogger");
            _messageDataTable = BuildMessageDataTable();
            _deleteMessagesByTimeAndMaxQueueOffsetFormat = "delete from [" + _setting.MessageTable + "] where Topic = '{0}' and QueueId = {1} and QueueOffset < {2} and StoredTime < '{3}'";
            _selectMaxMessageOffsetSQL = "select max(MessageOffset) from [" + _setting.MessageTable + "]";
            _selectAllMessageSQL = "select * from [" + _setting.MessageTable + "] where MessageOffset > {0} order by MessageOffset asc";
            _batchLoadMessageSQLFormat = "select * from [" + _setting.MessageTable + "] where MessageOffset >= {0} and MessageOffset < {1}";
            _batchLoadQueueIndexSQLFormat = "select QueueOffset,MessageOffset from [" + _setting.MessageTable + "] where Topic = '{0}' and QueueId = {1} and QueueOffset >= {2} and QueueOffset < {3}";
            _getMessageOffsetByQueueOffset = "select MessageOffset from [" + _setting.MessageTable + "] where Topic = '{0}' and QueueId = {1} and QueueOffset = {2}";
            _taskIds = new List<int>();
        }

        public void Recover(IEnumerable<QueueConsumedOffset> queueConsumedOffsets, Action<long, string, int, long> messageRecoveredCallback)
        {
            Clear();
            var totalRecoveredMessageCount = RecoverAllMessagesFromDB(queueConsumedOffsets, messageRecoveredCallback);
            if (totalRecoveredMessageCount > 0)
            {
                RecoverAllMessagesFromMessageLogs(messageRecoveredCallback);
            }
            else
            {
                RecoverMaxMessageOffsetFromMessageLogs();
            }
        }
        public void Start()
        {
            _taskIds.Add(_scheduleService.ScheduleTask("SqlServerMessageStore.TryPersistMessages", TryPersistMessages, _setting.PersistMessageInterval, _setting.PersistMessageInterval));
            _taskIds.Add(_scheduleService.ScheduleTask("SqlServerMessageStore.RemoveExceedMaxCacheMessageFromMemory", RemoveExceedMaxCacheMessageFromMemory, _setting.RemoveExceedMaxCacheMessageFromMemoryInterval, _setting.RemoveExceedMaxCacheMessageFromMemoryInterval));
            _taskIds.Add(_scheduleService.ScheduleTask("SqlServerMessageStore.RemoveConsumedMessageFromMemory", RemoveConsumedMessageFromMemory, _setting.RemoveConsumedMessageFromMemoryInterval, _setting.RemoveConsumedMessageFromMemoryInterval));
            _taskIds.Add(_scheduleService.ScheduleTask("SqlServerMessageStore.DeleteMessages", DeleteMessages, _setting.DeleteMessageInterval, _setting.DeleteMessageInterval));
        }
        public void Shutdown()
        {
            foreach (var taskId in _taskIds)
            {
                _scheduleService.ShutdownTask(taskId);
            }
        }
        public long GetNextMessageOffset()
        {
            return Interlocked.Increment(ref _currentMessageOffset);
        }
        public QueueMessage StoreMessage(int queueId, long messageOffset, long queueOffset, Message message, string routingKey)
        {
            var queueMessage = new QueueMessage(
                ObjectId.GenerateNewStringId(),
                message.Topic,
                message.Code,
                message.Body,
                messageOffset,
                queueId,
                queueOffset,
                message.CreatedTime,
                DateTime.Now,
                DateTime.Now,
                routingKey);
            _messageDict[messageOffset] = queueMessage;
            WriteMessageLog(queueMessage);
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
        public QueueMessage FindMessage(long? offset, string messageId)
        {
            var whereSql = string.Empty;
            var hasCondition = false;

            if (offset != null)
            {
                whereSql += string.Format(" where MessageOffset = {0}", offset.Value);
                hasCondition = true;
            }
            if (!string.IsNullOrWhiteSpace(messageId))
            {
                var prefix = hasCondition ? " and " : " where ";
                whereSql += prefix + string.Format("MessageId = '{0}'", messageId);
            }

            using (var connection = new SqlConnection(_setting.ConnectionString))
            {
                connection.Open();
                var sql = string.Format("select * from {0}{1}", _setting.MessageTable, whereSql);
                using (var command = new SqlCommand(sql, connection))
                {
                    var reader = command.ExecuteReader();
                    if (reader.Read())
                    {
                        return PopulateMessageFromReader(reader);
                    }
                }
            }

            return null;
        }
        public void UpdateConsumedQueueOffset(string topic, int queueId, long queueOffset)
        {
            var key = string.Format("{0}-{1}", topic, queueId);
            _queueConsumedOffsetDict.AddOrUpdate(key, queueOffset, (currentKey, oldOffset) => queueOffset > oldOffset ? queueOffset : oldOffset);
        }
        public void DeleteQueueMessage(string topic, int queueId)
        {
            var key = string.Format("{0}-{1}", topic, queueId);
            var messages = _messageDict.Values.Where(x => x.Topic == topic && x.QueueId == queueId);
            messages.ForEach(x => _messageDict.Remove(x.MessageOffset));
            _queueConsumedOffsetDict.Remove(key);
            _queueMaxPersistedOffsetDict.Remove(key);
            DeleteMessagesByTimeAndMaxQueueOffset(topic, queueId, long.MaxValue, DateTime.MaxValue);
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
                if (!hasCondition)
                {
                    hasCondition = true;
                }
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
                            SELECT ROW_NUMBER() OVER (ORDER BY m.MessageOffset desc) AS RowNumber,m.MessageId,m.MessageOffset,m.Topic,m.QueueId,m.QueueOffset,m.Code,m.CreatedTime,m.ArrivedTime,m.StoredTime,m.RoutingKey
                            FROM {0} m{1}) AS Total
                        WHERE RowNumber >= {2} AND RowNumber <= {3}",
                    _setting.MessageTable, whereSql, (pageIndex - 1) * pageSize + 1, pageIndex * pageSize);

                using (var command = new SqlCommand(pageSql, connection))
                {
                    var reader = command.ExecuteReader();
                    var messages = new List<QueueMessage>();
                    while (reader.Read())
                    {
                        messages.Add(PopulateMessageFromReader(reader));
                    }
                    return messages;
                }
            }
        }

        private void Clear()
        {
            _messageDict.Clear();
            _queueConsumedOffsetDict.Clear();
            _queueMaxPersistedOffsetDict.Clear();
            _messageDataTable.Rows.Clear();
            _currentMessageOffset = -1;
            _persistedMessageOffset = -1;
        }
        private long GetMinConsumedMessageOffset(IEnumerable<QueueConsumedOffset> queueConsumedOffsets)
        {
            var messageOffsetList = new List<long>();
            foreach (var queueConsumedOffset in queueConsumedOffsets)
            {
                messageOffsetList.Add(GetMessageOffsetByQueueOffset(queueConsumedOffset.Topic, queueConsumedOffset.QueueId, queueConsumedOffset.ConsumedOffset));
            }
            if (messageOffsetList.IsNotEmpty())
            {
                return messageOffsetList.Min();
            }
            return -1L;
        }
        private long GetMessageOffsetByQueueOffset(string topic, int queueId, long queueOffset)
        {
            using (var connection = new SqlConnection(_setting.ConnectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(string.Format(_getMessageOffsetByQueueOffset, topic, queueId, queueOffset), connection))
                {
                    var reader = command.ExecuteReader();
                    if (reader.Read())
                    {
                        return (long)reader["MessageOffset"];
                    }
                    return -1L;
                }
            }
        }
        private void RemoveExceedMaxCacheMessageFromMemory()
        {
            var currentTotalCount = _messageDict.Count;
            var exceedCount = currentTotalCount - _setting.MessageMaxCacheSize;
            if (exceedCount > 0)
            {
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
                    _logger.InfoFormat("Auto removed {0} unconsumed messages which exceed the max cache size, current total unconsumed message count:{1}, current exceed count:{2}, currentMessageOffset:{3}, currentPersistedMessageOffset:{4}", totalRemovedCount, currentTotalCount, exceedCount, currentMessageOffset, currentPersistedMessageOffet);
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
                    var maxRemovedMessageOffset = 0L;
                    foreach (var queueMessage in _messageDict.Values)
                    {
                        var key = string.Format("{0}-{1}", queueMessage.Topic, queueMessage.QueueId);
                        var queueConsumedOffset = 0L;
                        if (_queueConsumedOffsetDict.TryGetValue(key, out queueConsumedOffset)
                            && queueMessage.MessageOffset <= _persistedMessageOffset
                            && queueMessage.QueueOffset <= queueConsumedOffset)
                        {
                            QueueMessage removedMessage;
                            if (_messageDict.TryRemove(queueMessage.MessageOffset, out removedMessage))
                            {
                                totalRemovedCount++;
                                if (removedMessage.MessageOffset > maxRemovedMessageOffset)
                                {
                                    maxRemovedMessageOffset = removedMessage.MessageOffset;
                                }
                            }
                        }
                    }
                    if (totalRemovedCount > 0)
                    {
                        _logger.InfoFormat("Auto removed {0} consumed messages from memory, maxRemovedMessageOffset: {1}, currentPersistedMessageOffset: {2}", totalRemovedCount, maxRemovedMessageOffset, _persistedMessageOffset);
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
        private long GetMaxMessageOffset()
        {
            using (var connection = new SqlConnection(_setting.ConnectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(_selectMaxMessageOffsetSQL, connection))
                {
                    var result = command.ExecuteScalar();
                    if (result != null && result != DBNull.Value)
                    {
                        return (long)result;
                    }
                    return -1L;
                }
            }
        }
        private long RecoverAllMessagesFromDB(IEnumerable<QueueConsumedOffset> queueConsumedOffsets, Action<long, string, int, long> messageRecoveredCallback)
        {
            _logger.Info("Start to recover messages from db.");
            var maxMessageOffset = GetMaxMessageOffset();
            var minConsumedMessageOffset = GetMinConsumedMessageOffset(queueConsumedOffsets);
            var startRecoveryMessageOffset = Math.Min(maxMessageOffset - 1, minConsumedMessageOffset - 1);
            if (startRecoveryMessageOffset < -1)
            {
                startRecoveryMessageOffset = -1;
            }
            _logger.InfoFormat("maxMessageOffset: {0}, minConsumedMessageOffset: {1}, startRecoveryMessageOffset: {2}", maxMessageOffset, minConsumedMessageOffset, startRecoveryMessageOffset);

            var totalRecoveredMessageCount = 0L;
            using (var connection = new SqlConnection(_setting.ConnectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(string.Format(_selectAllMessageSQL, startRecoveryMessageOffset), connection))
                {
                    var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        var queueMessage = PopulateMessageFromReader(reader, true);
                        RecoverMessageToMemory(queueMessage);
                        messageRecoveredCallback(queueMessage.MessageOffset, queueMessage.Topic, queueMessage.QueueId, queueMessage.QueueOffset);
                        totalRecoveredMessageCount++;
                    }
                    _logger.InfoFormat("{0} messages recovered, current messageOffset:{1}", totalRecoveredMessageCount, _currentMessageOffset);
                }
            }
            return totalRecoveredMessageCount;
        }
        private void RecoverAllMessagesFromMessageLogs(Action<long, string, int, long> messageRecoveredCallback)
        {
            _logger.Info("Start to recover messages from message log file.");
            var beginMessageOffset = _currentMessageOffset + 1;
            var messageFiles = GetRecoverMessageFiles(beginMessageOffset);
            if (messageFiles == null || messageFiles.IsEmpty())
            {
                _logger.Info("No messages need to recover from message log file.");
                return;
            }

            var totalRecoveredMessageCount = 0L;
            foreach (var messageFile in messageFiles)
            {
                var queueMessages = messageFile.GetMessages(beginMessageOffset);
                foreach (var queueMessage in queueMessages)
                {
                    PersistMessage(queueMessage);
                    RecoverMessageToMemory(queueMessage);
                    messageRecoveredCallback(queueMessage.MessageOffset, queueMessage.Topic, queueMessage.QueueId, queueMessage.QueueOffset);
                }
                _logger.InfoFormat("Recovered {0} messages from message log file '{1}' to db, current messageOffset: {2}", queueMessages.Count(), messageFile.FileName, _currentMessageOffset);
                totalRecoveredMessageCount += queueMessages.Count();
            }
            _logger.InfoFormat("Total recovered {0} messages from message log file to db, current messageOffset: {1}", totalRecoveredMessageCount, _currentMessageOffset);
        }
        private void RecoverMaxMessageOffsetFromMessageLogs()
        {
            _logger.Info("No messages recovered from db, try to recover the max message offset from message log file.");

            var fileName = Path.Combine(_setting.MessageLogFilePath, MessageFileName);
            if (!new FileInfo(fileName).Exists)
            {
                _logger.InfoFormat("Message log file not exist, fileName: {0}", fileName);
                return;
            }

            var message = GetLastLineMessage(fileName);
            if (message != null)
            {
                _currentMessageOffset = message.MessageOffset;
                _persistedMessageOffset = message.MessageOffset;
                _logger.InfoFormat("Recovered maxMessageOffset: {0}", _currentMessageOffset);
                return;
            }

            _logger.Info("No message found in the message file.");
        }
        private void WriteMessageLog(QueueMessage message)
        {
            var messageData = new MessageData
            {
                MessageId = message.MessageId,
                MessageOffset = message.MessageOffset,
                Topic = message.Topic,
                QueueId = message.QueueId,
                QueueOffset = message.QueueOffset,
                RoutingKey = message.RoutingKey,
                Code = message.Code,
                Body = ObjectId.ToHexString(message.Body),
                Created = message.CreatedTime,
                Arrived = message.ArrivedTime
            };
            _messageLogger.Info(_jsonSerializer.Serialize(messageData));
        }
        private IEnumerable<MessageFile> GetRecoverMessageFiles(long beginMessageOffset)
        {
            if (beginMessageOffset == 0L)
            {
                _logger.Info("The database does not has any messages, try to recover all the messages from all the local message log files.");
            }
            var stack = new Stack<MessageFile>();
            var messageFiles = new List<MessageFile>();
            var fileName = Path.Combine(_setting.MessageLogFilePath, MessageFileName);
            if (!new FileInfo(fileName).Exists)
            {
                _logger.InfoFormat("Message log file not exist, fileName: {0}", fileName);
                return messageFiles;
            }
            var index = 0;
            var message = GetFirstLineMessage(fileName);
            var currentFileName = fileName;
            while (message != null)
            {
                stack.Push(new MessageFile(_logger, _jsonSerializer, currentFileName));
                if (message.MessageOffset <= beginMessageOffset)
                {
                    break;
                }
                index++;
                currentFileName = fileName + "." + index;
                if (!new FileInfo(currentFileName).Exists)
                {
                    if (beginMessageOffset > 0L)
                    {
                        _logger.ErrorFormat("Message log file not exist, messages offset from [{0}] to [{1}] will not be recovered. fileName: {2}", beginMessageOffset, message.MessageOffset - 1, currentFileName);
                    }
                    break;
                }
                message = GetFirstLineMessage(currentFileName);
            }

            while (stack.IsNotEmpty())
            {
                messageFiles.Add(stack.Pop());
            }
            return messageFiles;
        }
        private QueueMessage GetFirstLineMessage(string fileName)
        {
            using (var fs = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                using (var reader = new StreamReader(fs, Encoding.UTF8))
                {
                    var line = reader.ReadLine();
                    while (line != null && string.IsNullOrWhiteSpace(line))
                    {
                        line = reader.ReadLine();
                    }
                    if (!string.IsNullOrWhiteSpace(line))
                    {
                        return ParseMessage(fileName, line);
                    }
                }
            }
            return null;
        }
        private QueueMessage GetLastLineMessage(string fileName)
        {
            using (var fs = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                using (var reader = new StreamReader(fs, Encoding.UTF8))
                {
                    var line = reader.ReadLine();
                    var lastLine = line;
                    while (line != null)
                    {
                        if (!string.IsNullOrWhiteSpace(line))
                        {
                            lastLine = line;
                        }
                        line = reader.ReadLine();
                    }
                    if (!string.IsNullOrWhiteSpace(lastLine))
                    {
                        return ParseMessage(fileName, lastLine);
                    }
                }
            }
            return null;
        }
        private QueueMessage ParseMessage(string fileName, string line)
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                return null;
            }
            var messageData = _jsonSerializer.Deserialize<MessageData>(line);
            return new QueueMessage(
                messageData.MessageId,
                messageData.Topic,
                messageData.Code,
                ObjectId.ParseHexString(messageData.Body),
                messageData.MessageOffset,
                messageData.QueueId,
                messageData.QueueOffset,
                messageData.Created,
                messageData.Arrived,
                messageData.Arrived,
                messageData.RoutingKey);
        }
        private void RecoverMessageToMemory(QueueMessage queueMessage)
        {
            if (_messageDict.Count < _setting.MessageMaxCacheSize)
            {
                _messageDict[queueMessage.MessageOffset] = queueMessage;
            }
            var key = string.Format("{0}-{1}", queueMessage.Topic, queueMessage.QueueId);
            long queueOffset;
            if (!_queueMaxPersistedOffsetDict.TryGetValue(key, out queueOffset) || queueOffset < queueMessage.QueueOffset)
            {
                _queueMaxPersistedOffsetDict[key] = queueMessage.QueueOffset;
            }
            if (_currentMessageOffset < queueMessage.MessageOffset)
            {
                _currentMessageOffset = queueMessage.MessageOffset;
                _persistedMessageOffset = queueMessage.MessageOffset;
            }
        }
        private void PersistMessage(QueueMessage message)
        {
            using (var connection = new SqlConnection(_setting.ConnectionString))
            {
                connection.Insert(new
                {
                    MessageId = message.MessageId,
                    MessageOffset = message.MessageOffset,
                    Topic = message.Topic,
                    QueueId = message.QueueId,
                    QueueOffset = message.QueueOffset,
                    Code = message.Code,
                    Body = message.Body,
                    RoutingKey = message.RoutingKey,
                    CreatedTime = message.CreatedTime,
                    ArrivedTime = message.ArrivedTime,
                    StoredTime = DateTime.Now
                }, _setting.MessageTable);
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
        private QueueMessage PopulateMessageFromReader(IDataReader reader, bool populateBody = false)
        {
            var messageId = (string)reader["MessageId"];
            var messageOffset = (long)reader["MessageOffset"];
            var topic = (string)reader["Topic"];
            var queueId = (int)reader["QueueId"];
            var queueOffset = (long)reader["QueueOffset"];
            var code = (int)reader["Code"];
            byte[] body = EmptyBody;
            if (populateBody)
            {
                body = (byte[])reader["Body"];
            }
            var createdTime = (DateTime)reader["CreatedTime"];
            var arrivedTime = (DateTime)reader["ArrivedTime"];
            var storedTime = (DateTime)reader["StoredTime"];
            var routingKey = (string)reader["RoutingKey"];
            return new QueueMessage(messageId, topic, code, body, messageOffset, queueId, queueOffset, createdTime, arrivedTime, storedTime, routingKey);
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
                    message.StoredTime = DateTime.Now;
                    messages.Add(message);
                }
                currentOffset++;
            }

            if (messages.Count == 0)
            {
                if (_persistedMessageOffset < _currentMessageOffset)
                {
                    _logger.WarnFormat("No messages were found to persist, but the persistedMessageOffset is small than the currentMaxMessageOffset. persistedMessageOffset: {0}, currentMaxMessageOffset: {1}", _persistedMessageOffset, _currentMessageOffset);
                }
                return true;
            }

            var queueMaxPersistedOffsetDict = new ConcurrentDictionary<string, long>();
            _messageDataTable.Rows.Clear();
            foreach (var message in messages)
            {
                var row = _messageDataTable.NewRow();
                row["MessageId"] = message.MessageId;
                row["MessageOffset"] = message.MessageOffset;
                row["Topic"] = message.Topic;
                row["QueueId"] = message.QueueId;
                row["QueueOffset"] = message.QueueOffset;
                row["Code"] = message.Code;
                row["Body"] = message.Body;
                row["CreatedTime"] = message.CreatedTime;
                row["ArrivedTime"] = message.ArrivedTime;
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
                    copy.ColumnMappings.Add("MessageId", "MessageId");
                    copy.ColumnMappings.Add("MessageOffset", "MessageOffset");
                    copy.ColumnMappings.Add("Topic", "Topic");
                    copy.ColumnMappings.Add("QueueId", "QueueId");
                    copy.ColumnMappings.Add("QueueOffset", "QueueOffset");
                    copy.ColumnMappings.Add("Code", "Code");
                    copy.ColumnMappings.Add("Body", "Body");
                    copy.ColumnMappings.Add("CreatedTime", "CreatedTime");
                    copy.ColumnMappings.Add("ArrivedTime", "ArrivedTime");
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
            table.Columns.Add("MessageId", typeof(string));
            table.Columns.Add("MessageOffset", typeof(long));
            table.Columns.Add("Topic", typeof(string));
            table.Columns.Add("QueueId", typeof(int));
            table.Columns.Add("QueueOffset", typeof(long));
            table.Columns.Add("Code", typeof(int));
            table.Columns.Add("Body", typeof(byte[]));
            table.Columns.Add("CreatedTime", typeof(DateTime));
            table.Columns.Add("ArrivedTime", typeof(DateTime));
            table.Columns.Add("StoredTime", typeof(DateTime));
            table.Columns.Add("RoutingKey", typeof(string));
            return table;
        }
        private void DeleteMessages()
        {
            var maxStoredTime = DateTime.Now.Subtract(TimeSpan.FromHours(_setting.MessageStoreMaxHours));

            if (_setting.IgnoreUnConsumedMessage)
            {
                foreach (var entry in _queueMaxPersistedOffsetDict)
                {
                    var items = entry.Key.Split(new string[] { "-" }, StringSplitOptions.None);
                    var topic = items[0];
                    var queueId = int.Parse(items[1]);
                    var maxAllowToDeleteQueueOffset = entry.Value;
                    DeleteMessagesByTimeAndMaxQueueOffset(topic, queueId, maxAllowToDeleteQueueOffset, maxStoredTime);
                }
            }
            else
            {
                foreach (var entry in _queueConsumedOffsetDict)
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
                    DeleteMessagesByTimeAndMaxQueueOffset(topic, queueId, maxAllowToDeleteQueueOffset, maxStoredTime);
                }
            }
        }
        private void DeleteMessagesByTimeAndMaxQueueOffset(string topic, int queueId, long maxAllowToDeleteQueueOffset, DateTime maxStoredTime)
        {
            using (var connection = new SqlConnection(_setting.ConnectionString))
            {
                connection.Open();
                using (var command = new SqlCommand(string.Format(_deleteMessagesByTimeAndMaxQueueOffsetFormat, topic, queueId, maxAllowToDeleteQueueOffset, maxStoredTime), connection))
                {
                    var deletedMessageCount = command.ExecuteNonQuery();
                    if (deletedMessageCount > 0)
                    {
                        _logger.DebugFormat("Deleted {0} messages, topic = {1}, queueId = {2}, queueOffset < {3}, storedTime < {4}.", deletedMessageCount, topic, queueId, maxAllowToDeleteQueueOffset, maxStoredTime);
                    }
                }
            }
            var allowToDeleteMessages = _messageDict.Values.Where(x => x.Topic == topic && x.QueueId == queueId && x.QueueOffset < maxAllowToDeleteQueueOffset && x.StoredTime < maxStoredTime);
            foreach (var message in allowToDeleteMessages)
            {
                _messageDict.Remove(message.MessageOffset);
            }
        }

        public class MessageFile
        {
            private readonly FileInfo _file;
            private readonly ILogger _logger;
            private readonly IJsonSerializer _jsonSerializer;

            public string FileName
            {
                get { return _file.Name; }
            }

            public MessageFile(ILogger logger, IJsonSerializer jsonSerializer, string fileName)
            {
                _file = new FileInfo(fileName);
                if (!_file.Exists)
                {
                    throw new Exception(string.Format("Message log file '{0}' not exist.", fileName));
                }
                _logger = logger;
                _jsonSerializer = jsonSerializer;
            }

            public IEnumerable<QueueMessage> GetMessages(long minMessageOffset)
            {
                using (var fs = new FileStream(_file.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    using (var reader = new StreamReader(fs, Encoding.UTF8))
                    {
                        var messages = new List<QueueMessage>();
                        string line;
                        while ((line = reader.ReadLine()) != null)
                        {
                            var message = ParseMessage(line);
                            if (message != null && message.MessageOffset >= minMessageOffset)
                            {
                                messages.Add(message);
                            }
                        }
                        return messages;
                    }
                }
            }
            private QueueMessage ParseMessage(string line)
            {
                if (string.IsNullOrWhiteSpace(line))
                {
                    return null;
                }
                var messageData = _jsonSerializer.Deserialize<MessageData>(line);
                return new QueueMessage(
                    messageData.MessageId,
                    messageData.Topic,
                    messageData.Code,
                    ObjectId.ParseHexString(messageData.Body),
                    messageData.MessageOffset,
                    messageData.QueueId,
                    messageData.QueueOffset,
                    messageData.Created,
                    messageData.Arrived,
                    messageData.Arrived,
                    messageData.RoutingKey);
            }
        }
        class MessageData
        {
            public string MessageId { get; set; }
            public long MessageOffset { get; set; }
            public string Topic { get; set; }
            public int QueueId { get; set; }
            public long QueueOffset { get; set; }
            public string RoutingKey { get; set; }
            public int Code { get; set; }
            public string Body { get; set; }
            public DateTime Created { get; set; }
            public DateTime Arrived { get; set; }
        }
    }
}
