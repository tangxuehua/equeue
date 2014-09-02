using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using ECommon.Components;
using ECommon.Logging;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class InMemoryOffsetManager : IOffsetManager
    {
        private ConcurrentDictionary<string, ConcurrentDictionary<string, long>> _dict = new ConcurrentDictionary<string, ConcurrentDictionary<string, long>>();
        private readonly ILogger _logger;

        public InMemoryOffsetManager()
        {
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void Recover() { }
        public void Start() { }
        public void Shutdown() { }

        public void UpdateQueueOffset(string topic, int queueId, long offset, string group)
        {
            var queueOffsetDict = _dict.GetOrAdd(group, new ConcurrentDictionary<string, long>());
            var key = string.Format("{0}-{1}", topic, queueId);
            queueOffsetDict.AddOrUpdate(key, offset, (currentKey, oldOffset) => offset > oldOffset ? offset : oldOffset);
        }
        public long GetQueueOffset(string topic, int queueId, string group)
        {
            ConcurrentDictionary<string, long> queueOffsetDict;
            if (_dict.TryGetValue(group, out queueOffsetDict))
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
            foreach (var queueOffsetDict in _dict.Values)
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
        public IEnumerable<TopicConsumeInfo> QueryTopicConsumeInfos(string groupName, string topic)
        {
            var entryList = _dict.Where(x => string.IsNullOrEmpty(groupName) || x.Key.Contains(groupName));
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
