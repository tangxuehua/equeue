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
using ECommon.Utilities;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Broker
{
    public class DefaultConsumeOffsetStore : IConsumeOffsetStore
    {
        private const string ConsumeOffsetFileName = "consume-offsets.json";
        private readonly IScheduleService _scheduleService;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly ILogger _logger;
        private readonly string _persistConsumeOffsetTaskName;
        private string _consumeOffsetFile;
        private ConcurrentDictionary<string, ConcurrentDictionary<string, long>> _groupConsumeOffsetsDict;
        private int _isPersistingOffsets;

        public DefaultConsumeOffsetStore(IScheduleService scheduleService, IJsonSerializer jsonSerializer, ILoggerFactory loggerFactory)
        {
            _groupConsumeOffsetsDict = new ConcurrentDictionary<string, ConcurrentDictionary<string, long>>();
            _scheduleService = scheduleService;
            _jsonSerializer = jsonSerializer;
            _logger = loggerFactory.Create(GetType().FullName);
            _persistConsumeOffsetTaskName = string.Format("{0}.PersistConsumeOffsetInfo", this.GetType().Name);
        }

        public void Clean()
        {
            CleanConsumeOffsets();
        }
        public void Start()
        {
            var path = BrokerController.Instance.Setting.FileStoreRootPath;
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
            _consumeOffsetFile = Path.Combine(path, ConsumeOffsetFileName);

            LoadConsumeOffsetInfo();
            _scheduleService.StartTask(_persistConsumeOffsetTaskName, PersistConsumeOffsetInfo, 1000 * 5,  BrokerController.Instance.Setting.PersistConsumeOffsetInterval);
        }
        public void Shutdown()
        {
            PersistConsumeOffsetInfo();
            _scheduleService.StopTask(_persistConsumeOffsetTaskName);
        }
        public int GetConsumerGroupCount()
        {
            return _groupConsumeOffsetsDict.Count;
        }
        public long GetConsumeOffset(string topic, int queueId, string group)
        {
            ConcurrentDictionary<string, long> queueOffsetDict;
            if (_groupConsumeOffsetsDict.TryGetValue(group, out queueOffsetDict))
            {
                long offset;
                var key = QueueKeyUtil.CreateQueueKey(topic, queueId);
                if (queueOffsetDict.TryGetValue(key, out offset))
                {
                    return offset;
                }
            }
            return -1L;
        }
        public long GetMinConsumedOffset(string topic, int queueId)
        {
            var key = QueueKeyUtil.CreateQueueKey(topic, queueId);
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
                return new ConcurrentDictionary<string, long>();
            });
            var key = QueueKeyUtil.CreateQueueKey(topic, queueId);
            queueOffsetDict.AddOrUpdate(key, offset, (currentKey, oldOffset) => offset > oldOffset ? offset : oldOffset);
        }
        public void DeleteConsumeOffset(string queueKey)
        {
            foreach (var dict in _groupConsumeOffsetsDict.Values)
            {
                var keys = dict.Keys.Where(x => x == queueKey);
                foreach (var key in keys)
                {
                    dict.Remove(key);
                }
            }
        }
        public IEnumerable<string> GetConsumeKeys()
        {
            var keyList = new List<string>();

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
        public IEnumerable<TopicConsumeInfo> QueryTopicConsumeInfos(string groupName, string topic)
        {
            var entryList = _groupConsumeOffsetsDict.Where(x => string.IsNullOrEmpty(groupName) || x.Key.Contains(groupName));
            var topicConsumeInfoList = new List<TopicConsumeInfo>();

            foreach (var entry in entryList)
            {
                foreach (var subEntry in entry.Value.Where(x => string.IsNullOrEmpty(topic) || QueueKeyUtil.ParseQueueKey(x.Key)[0].Contains(topic)))
                {
                    var items = QueueKeyUtil.ParseQueueKey(subEntry.Key);
                    topicConsumeInfoList.Add(new TopicConsumeInfo
                    {
                        ConsumerGroup = entry.Key,
                        Topic = items[0],
                        QueueId = int.Parse(items[1]),
                        ConsumedOffset = subEntry.Value
                    });
                }
            }

            return topicConsumeInfoList;
        }

        private void CleanConsumeOffsets()
        {
            var path = BrokerController.Instance.Setting.FileStoreRootPath;
            if (!Directory.Exists(path))
            {
                return;
            }

            _consumeOffsetFile = Path.Combine(path, ConsumeOffsetFileName);

            if (File.Exists(_consumeOffsetFile))
            {
                File.SetAttributes(_consumeOffsetFile, FileAttributes.Normal);
                File.Delete(_consumeOffsetFile);
            }

            _groupConsumeOffsetsDict.Clear();
        }
        private void LoadConsumeOffsetInfo()
        {
            using (var stream = new FileStream(_consumeOffsetFile, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                using (var reader = new StreamReader(stream))
                {
                    var json = reader.ReadToEnd();
                    if (!string.IsNullOrEmpty(json))
                    {
                        try
                        {
                            _groupConsumeOffsetsDict = _jsonSerializer.Deserialize<ConcurrentDictionary<string, ConcurrentDictionary<string, long>>>(json);
                        }
                        catch (Exception ex)
                        {
                            _logger.Error("Load consume offsets has exception.", ex);
                            throw;
                        }
                    }
                    else
                    {
                        _groupConsumeOffsetsDict = new ConcurrentDictionary<string, ConcurrentDictionary<string, long>>();
                    }
                }
            }
        }
        private void PersistConsumeOffsetInfo()
        {
            if (Interlocked.CompareExchange(ref _isPersistingOffsets, 1, 0) == 0)
            {
                try
                {
                    using (var stream = new FileStream(_consumeOffsetFile, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite))
                    {
                        using (var writer = new StreamWriter(stream))
                        {
                            var json = _jsonSerializer.Serialize(_groupConsumeOffsetsDict);
                            writer.Write(json);
                        }
                    }
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
    }
}
