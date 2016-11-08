using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Serializing;
using EQueue.Protocols.Brokers;

namespace EQueue.Broker
{
    public class DefaultConsumeOffsetStore : IConsumeOffsetStore
    {
        private const string ConsumeOffsetFileName = "consume-offsets.json";
        private const string ConsumeOffsetBackupFileName = "consume-offsets-backup.json";
        private readonly IScheduleService _scheduleService;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly ILogger _logger;
        private string _consumeOffsetFile;
        private string _consumeOffsetBackupFile;
        private ConcurrentDictionary<string /*groupName*/, ConcurrentDictionary<QueueKey, long>> _groupConsumeOffsetsDict;
        private ConcurrentDictionary<string /*groupName*/, ConcurrentDictionary<QueueKey, long>> _groupNextConsumeOffsetsDict;
        private int _isPersistingOffsets;

        public DefaultConsumeOffsetStore(IScheduleService scheduleService, IJsonSerializer jsonSerializer, ILoggerFactory loggerFactory)
        {
            _groupConsumeOffsetsDict = new ConcurrentDictionary<string, ConcurrentDictionary<QueueKey, long>>();
            _groupNextConsumeOffsetsDict = new ConcurrentDictionary<string, ConcurrentDictionary<QueueKey, long>>();
            _scheduleService = scheduleService;
            _jsonSerializer = jsonSerializer;
            _logger = loggerFactory.Create(GetType().FullName);
        }

        public void Start()
        {
            if (BrokerController.Instance.Setting.IsMessageStoreMemoryMode)
            {
                return;
            }

            var path = BrokerController.Instance.Setting.FileStoreRootPath;
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
            _consumeOffsetFile = Path.Combine(path, ConsumeOffsetFileName);
            _consumeOffsetBackupFile = Path.Combine(path, ConsumeOffsetBackupFileName);

            LoadConsumeOffsetInfo();
            _scheduleService.StartTask("PersistConsumeOffsetInfo", PersistConsumeOffsetInfo, 1000 * 5, BrokerController.Instance.Setting.PersistConsumeOffsetInterval);
        }
        public void Shutdown()
        {
            if (BrokerController.Instance.Setting.IsMessageStoreMemoryMode)
            {
                return;
            }
            PersistConsumeOffsetInfo();
            _scheduleService.StopTask("PersistConsumeOffsetInfo");
        }
        public int GetConsumerGroupCount()
        {
            return _groupConsumeOffsetsDict.Count;
        }
        public IEnumerable<string> GetAllConsumerGroupNames()
        {
            return _groupConsumeOffsetsDict.Keys.ToList();
        }
        public bool DeleteConsumerGroup(string group)
        {
            ConcurrentDictionary<QueueKey, long> queueOffsetDict;
            return _groupConsumeOffsetsDict.TryRemove(group, out queueOffsetDict);
        }
        public long GetConsumeOffset(string topic, int queueId, string group)
        {
            ConcurrentDictionary<QueueKey, long> queueOffsetDict;
            if (_groupConsumeOffsetsDict.TryGetValue(group, out queueOffsetDict))
            {
                long offset;
                var key = new QueueKey(topic, queueId);
                if (queueOffsetDict.TryGetValue(key, out offset))
                {
                    return offset;
                }
            }
            return -1L;
        }
        public long GetMinConsumedOffset(string topic, int queueId)
        {
            var key = new QueueKey(topic, queueId);
            var minOffset = -1L;

            foreach (var queueOffsetDict in _groupConsumeOffsetsDict.Values)
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
        public void UpdateConsumeOffset(string topic, int queueId, long offset, string group)
        {
            var queueOffsetDict = _groupConsumeOffsetsDict.GetOrAdd(group, k =>
            {
                return new ConcurrentDictionary<QueueKey, long>();
            });
            var key = new QueueKey(topic, queueId);
            queueOffsetDict[key] = offset;
        }
        public void DeleteConsumeOffset(QueueKey queueKey)
        {
            foreach (var dict in _groupConsumeOffsetsDict.Values)
            {
                var keys = dict.Keys.Where(x => x == queueKey);
                foreach (var key in keys)
                {
                    dict.Remove(key);
                }
            }
            PersistConsumeOffsetInfo();
        }
        public IEnumerable<QueueKey> GetConsumeKeys()
        {
            var keyList = new List<QueueKey>();

            foreach (var dict in _groupConsumeOffsetsDict.Values)
            {
                foreach (var key in dict.Keys)
                {
                    if (!keyList.Contains(key))
                    {
                        keyList.Add(key);
                    }
                }
            }

            return keyList;
        }
        public void SetConsumeNextOffset(string topic, int queueId, string group, long nextOffset)
        {
            var queueOffsetDict = _groupNextConsumeOffsetsDict.GetOrAdd(group, k =>
            {
                return new ConcurrentDictionary<QueueKey, long>();
            });
            var key = new QueueKey(topic, queueId);
            queueOffsetDict[key] = nextOffset;
        }
        public bool TryFetchNextConsumeOffset(string topic, int queueId, string group, out long nextOffset)
        {
            nextOffset = 0L;
            ConcurrentDictionary<QueueKey, long> queueOffsetDict;
            if (_groupNextConsumeOffsetsDict.TryGetValue(group, out queueOffsetDict))
            {
                long offset;
                var key = new QueueKey(topic, queueId);
                if (queueOffsetDict.TryRemove(key, out offset))
                {
                    nextOffset = offset;
                    return true;
                }
            }
            return false;
        }
        public IEnumerable<TopicConsumeInfo> GetAllTopicConsumeInfoList()
        {
            var topicConsumeInfoList = new List<TopicConsumeInfo>();
            foreach (var entry in _groupConsumeOffsetsDict)
            {
                var groupName = entry.Key;
                var consumeInfoDict = entry.Value;
                foreach (var entry2 in consumeInfoDict)
                {
                    var queueKey = entry2.Key;
                    var consumedOffset = entry2.Value;
                    topicConsumeInfoList.Add(new TopicConsumeInfo
                    {
                        ConsumerGroup = groupName,
                        Topic = queueKey.Topic,
                        QueueId = queueKey.QueueId,
                        ConsumedOffset = consumedOffset
                    });
                }
            }
            topicConsumeInfoList.Sort(SortTopicConsumeInfo);
            return topicConsumeInfoList;
        }
        public IEnumerable<TopicConsumeInfo> GetTopicConsumeInfoList(string groupName, string topic)
        {
            var topicConsumeInfoList = new List<TopicConsumeInfo>();
            if (!string.IsNullOrEmpty(groupName) && !string.IsNullOrEmpty(topic))
            {
                ConcurrentDictionary<QueueKey, long> found;
                if (_groupConsumeOffsetsDict.TryGetValue(groupName, out found))
                {
                    foreach (var entry in found.Where(x => x.Key.Topic == topic))
                    {
                        var queueKey = entry.Key;
                        var consumedOffset = entry.Value;
                        topicConsumeInfoList.Add(new TopicConsumeInfo
                        {
                            ConsumerGroup = groupName,
                            Topic = queueKey.Topic,
                            QueueId = queueKey.QueueId,
                            ConsumedOffset = consumedOffset
                        });
                    }
                }
            }
            topicConsumeInfoList.Sort(SortTopicConsumeInfo);
            return topicConsumeInfoList;
        }

        private int SortTopicConsumeInfo(TopicConsumeInfo x, TopicConsumeInfo y)
        {
            var result = string.Compare(x.ConsumerGroup, y.ConsumerGroup);
            if (result != 0)
            {
                return result;
            }
            result = string.Compare(x.Topic, y.Topic);
            if (result != 0)
            {
                return result;
            }
            if (x.QueueId > y.QueueId)
            {
                return 1;
            }
            else if (x.QueueId < y.QueueId)
            {
                return -1;
            }
            return 0;
        }
        private void LoadConsumeOffsetInfo()
        {
            try
            {
                LoadConsumeOffsetInfo(_consumeOffsetFile);
            }
            catch
            {
                LoadConsumeOffsetInfo(_consumeOffsetBackupFile);
            }
        }
        private void LoadConsumeOffsetInfo(string file)
        {
            using (var stream = new FileStream(file, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                using (var reader = new StreamReader(stream))
                {
                    var json = reader.ReadToEnd();
                    if (!string.IsNullOrEmpty(json))
                    {
                        try
                        {
                            var dict = _jsonSerializer.Deserialize<ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<int, long>>>>(json);
                            _groupConsumeOffsetsDict = ConvertDictFrom(dict);
                        }
                        catch (Exception ex)
                        {
                            _logger.Error("Deserialize consume offsets failed, file:" + file, ex);
                            throw;
                        }
                    }
                    else
                    {
                        _groupConsumeOffsetsDict = new ConcurrentDictionary<string, ConcurrentDictionary<QueueKey, long>>();
                    }
                }
            }
        }
        private void PersistConsumeOffsetInfo()
        {
            if (string.IsNullOrEmpty(_consumeOffsetFile))
            {
                return;
            }
            if (Interlocked.CompareExchange(ref _isPersistingOffsets, 1, 0) == 0)
            {
                try
                {
                    using (var stream = new FileStream(_consumeOffsetFile, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite))
                    {
                        using (var writer = new StreamWriter(stream))
                        {
                            var toDict = ConvertDictTo(_groupConsumeOffsetsDict);
                            var json = _jsonSerializer.Serialize(toDict);
                            writer.Write(json);
                            writer.Flush();
                            stream.Flush(true);
                        }
                    }
                    File.Copy(_consumeOffsetFile, _consumeOffsetBackupFile, true);
                }
                catch (Exception ex)
                {
                    _logger.Error("Persist consume offsets has exception.", ex);
                }
                finally
                {
                    Interlocked.Exchange(ref _isPersistingOffsets, 0);
                }
            }
        }
        private ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<int, long>>> ConvertDictTo(ConcurrentDictionary<string, ConcurrentDictionary<QueueKey, long>> source)
        {
            var toDict = new ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<int, long>>>();

            foreach (var entry1 in source)
            {
                var key1 = entry1.Key;
                var dict = toDict.GetOrAdd(key1, x => new ConcurrentDictionary<string, ConcurrentDictionary<int, long>>());

                foreach (var entry2 in entry1.Value)
                {
                    var topic = entry2.Key.Topic;
                    var queueId = entry2.Key.QueueId;
                    var offset = entry2.Value;
                    var dict2 = dict.GetOrAdd(topic, x => new ConcurrentDictionary<int, long>());
                    dict2.TryAdd(queueId, offset);
                }
            }

            return toDict;
        }
        private ConcurrentDictionary<string, ConcurrentDictionary<QueueKey, long>> ConvertDictFrom(ConcurrentDictionary<string, ConcurrentDictionary<string, ConcurrentDictionary<int, long>>> source)
        {
            var toDict = new ConcurrentDictionary<string, ConcurrentDictionary<QueueKey, long>>();

            foreach (var entry1 in source)
            {
                var key1 = entry1.Key;
                var dict = toDict.GetOrAdd(key1, x => new ConcurrentDictionary<QueueKey, long>());

                foreach (var entry2 in entry1.Value)
                {
                    var topic = entry2.Key;
                    foreach (var entry3 in entry2.Value)
                    {
                        var queueId = entry3.Key;
                        var offset = entry3.Value;
                        var queueKey = new QueueKey(topic, queueId);
                        dict.TryAdd(queueKey, offset);
                    }
                }
            }

            return toDict;
        }
    }
}
