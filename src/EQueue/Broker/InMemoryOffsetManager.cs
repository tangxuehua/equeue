using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.Logging;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class InMemoryOffsetManager : IOffsetManager
    {
        private ConcurrentDictionary<string, ConcurrentDictionary<string, long>> _groupQueueOffsetDict = new ConcurrentDictionary<string, ConcurrentDictionary<string, long>>();
        private readonly ILogger _logger;

        public InMemoryOffsetManager()
        {
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void Recover() { }
        public void Start() { }
        public void Shutdown() { }

        public int GetConsumerGroupCount()
        {
            return _groupQueueOffsetDict.Keys.Count;
        }
        public void DeleteQueue(string topic, int queueId)
        {
            var key = string.Format("{0}-{1}", topic, queueId);
            foreach (var groupEntry in _groupQueueOffsetDict)
            {
                long offset;
                if (groupEntry.Value.TryRemove(key, out offset))
                {
                    _logger.DebugFormat("Deleted queue offset, topic:{0}, queueId:{1}, consumer group:{2}, consumedOffset:{3}", topic, queueId, groupEntry.Key, offset);
                }
            }
        }
        public void UpdateQueueOffset(string topic, int queueId, long offset, string group)
        {
            var queueOffsetDict = _groupQueueOffsetDict.GetOrAdd(group, new ConcurrentDictionary<string, long>());
            var key = string.Format("{0}-{1}", topic, queueId);
            queueOffsetDict.AddOrUpdate(key, offset, (currentKey, oldOffset) => offset > oldOffset ? offset : oldOffset);
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
        public void RemoveQueueOffset(string consumerGroup, string topic, int queueId)
        {
            ConcurrentDictionary<string, long> queueOffsetDict;
            if (_groupQueueOffsetDict.TryGetValue(consumerGroup, out queueOffsetDict))
            {
                var key = string.Format("{0}-{1}", topic, queueId);
                long offset;
                if (queueOffsetDict.TryRemove(key, out offset))
                {
                    _logger.DebugFormat("Remove queue offset success, topic:{0}, queueId:{1}, consumer group:{2}", topic, queueId, consumerGroup);
                }
            }
        }
        public IEnumerable<TopicConsumeInfo> QueryTopicConsumeInfos(string groupName, string topic)
        {
            var entryList = _groupQueueOffsetDict.Where(x => string.IsNullOrEmpty(groupName) || x.Key.Contains(groupName));
            var topicConsumeInfoList = new List<TopicConsumeInfo>();

            foreach (var entry in entryList)
            {
                foreach (var subEntry in entry.Value.Where(x => string.IsNullOrEmpty(topic) || x.Key.Split(new string[] { "-" }, StringSplitOptions.None)[0].Contains(topic)))
                {
                    var items = subEntry.Key.Split(new string[] { "-" }, StringSplitOptions.None);
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
    }
}
