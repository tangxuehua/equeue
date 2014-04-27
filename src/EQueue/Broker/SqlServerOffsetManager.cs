using System;
using System.Collections.Concurrent;
using System.Data.SqlClient;
using System.Threading;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Scheduling;

namespace EQueue.Broker
{
    public class SqlServerOffsetManager : IOffsetManager
    {
        private ConcurrentDictionary<string, ConcurrentDictionary<string, long>> _groupQueueOffsetDict = new ConcurrentDictionary<string, ConcurrentDictionary<string, long>>();
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;
        private int _persistQueueOffsetTaskId;
        private SqlServerOffsetManagerSetting _setting;
        private string _getLatestVersionSQL;
        private string _getLatestVersionQueueOffsetSQL;
        private string _insertNewVersionQueueOffsetSQLFormat;
        private string _deleteOldVersionQueueOffsetSQLFormat;
        private long _currentVersion;
        private long _lastUpdateVersion;
        private long _lastPersistVersion;

        public SqlServerOffsetManager(SqlServerOffsetManagerSetting setting)
        {
            _setting = setting;
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _getLatestVersionSQL = "select max(Version) from [" + _setting.QueueOffsetTable + "]";
            _getLatestVersionQueueOffsetSQL = "select * from [" + _setting.QueueOffsetTable + "] where Version = {0}";
            _insertNewVersionQueueOffsetSQLFormat = "insert into [" + _setting.QueueOffsetTable + "] (Version,ConsumerGroup,Topic,QueueId,QueueOffset,Timestamp) values ({0},'{1}','{2}',{3},{4},'{5}')";
            _deleteOldVersionQueueOffsetSQLFormat = "delete from [" + _setting.QueueOffsetTable + "] where Version = {0}";
        }

        public void Recover()
        {
            RecoverQueueOffset();
        }
        public void Start()
        {
            _lastUpdateVersion = 0;
            _lastPersistVersion = 0;
            _persistQueueOffsetTaskId = _scheduleService.ScheduleTask(PersistQueueOffset, _setting.CommitQueueOffsetInterval, _setting.CommitQueueOffsetInterval);
        }
        public void Shutdown()
        {
            _scheduleService.ShutdownTask(_persistQueueOffsetTaskId);
        }

        public void UpdateQueueOffset(string topic, int queueId, long offset, string group)
        {
            var queueOffsetDict = _groupQueueOffsetDict.GetOrAdd(group, new ConcurrentDictionary<string, long>());
            var key = string.Format("{0}-{1}", topic, queueId);
            queueOffsetDict.AddOrUpdate(key, offset, (currentKey, oldOffset) => offset > oldOffset ? offset : oldOffset);
            Interlocked.Increment(ref _lastUpdateVersion);
        }
        public long GetQueueOffset(string topic, int queueId, string group)
        {
            ConcurrentDictionary<string, long> queueOffsetDict;
            if (_groupQueueOffsetDict.TryGetValue(group, out queueOffsetDict))
            {
                long offset;
                var key = string.Format("{0}-{1}", topic, queueId);
                if (queueOffsetDict.TryGetValue(key, out offset))
                {
                    return offset;
                }
            }
            return -1L;
        }
        public long GetMinOffset(string topic, int queueId)
        {
            var key = string.Format("{0}-{1}", topic, queueId);
            var minOffset = -1L;
            foreach (var queueOffsetDict in _groupQueueOffsetDict.Values)
            {
                long offset;
                if (queueOffsetDict.TryGetValue(key, out offset))
                {
                    if (minOffset == -1)
                    {
                        minOffset = offset;
                    }
                    else if (offset < minOffset)
                    {
                        minOffset = offset;
                    }
                }
            }

            return minOffset;
        }

        private void RecoverQueueOffset()
        {
            _currentVersion = 0;
            _lastUpdateVersion = 0;
            _lastPersistVersion = 0;
            _groupQueueOffsetDict.Clear();

            using (var connection = new SqlConnection(_setting.ConnectionString))
            {
                connection.Open();

                long? maxVersion;
                using (var command = new SqlCommand(_getLatestVersionSQL, connection))
                {
                    var result = command.ExecuteScalar();
                    if (result == null || result == DBNull.Value)
                    {
                        return;
                    }
                    maxVersion = (long)result;
                }

                if (maxVersion == null)
                {
                    return;
                }

                _currentVersion = maxVersion.Value;

                using (var command = new SqlCommand(string.Format(_getLatestVersionQueueOffsetSQL, maxVersion), connection))
                {
                    var reader = command.ExecuteReader();
                    var count = 0L;
                    while (reader.Read())
                    {
                        var version = (long)reader["Version"];
                        var group = (string)reader["ConsumerGroup"];
                        var topic = (string)reader["Topic"];
                        var queueId = (int)reader["QueueId"];
                        var queueOffset = (long)reader["QueueOffset"];

                        UpdateQueueOffset(topic, queueId, queueOffset, group);
                        count++;
                    }
                    _logger.InfoFormat("{0} queue offset records recovered, current queue offset version:{1}", count, _currentVersion);
                }
            }
        }
        private void PersistQueueOffset()
        {
            var lastUpdateVersion = _lastUpdateVersion;
            if (_lastPersistVersion >= lastUpdateVersion)
            {
                return;
            }
            using (var connection = new SqlConnection(_setting.ConnectionString))
            {
                connection.Open();

                //Start the sql transaction.
                var transaction = connection.BeginTransaction();

                //Insert the new version of queueOffset.
                var timestamp = DateTime.Now;
                using (var command = new SqlCommand())
                {
                    command.Connection = connection;
                    command.Transaction = transaction;
                    foreach (var groupEntry in _groupQueueOffsetDict)
                    {
                        var group = groupEntry.Key;
                        foreach (var offsetEntry in groupEntry.Value)
                        {
                            var items = offsetEntry.Key.Split(new string[] { "-" }, StringSplitOptions.None);
                            var topic = items[0];
                            var queueId = items[1];
                            var queueOffset = offsetEntry.Value;
                            var version = _currentVersion + 1;
                            command.CommandText = string.Format(_insertNewVersionQueueOffsetSQLFormat, version, group, topic, queueId, queueOffset, timestamp);
                            command.ExecuteNonQuery();
                        }
                    }
                    //Delete the old version of queueOffset.
                    command.CommandText = string.Format(_deleteOldVersionQueueOffsetSQLFormat, _currentVersion);
                    command.ExecuteNonQuery();
                }

                //Commit the sql transaction.
                transaction.Commit();

                _currentVersion++;
                _lastPersistVersion = lastUpdateVersion;
            }
        }
    }
}
