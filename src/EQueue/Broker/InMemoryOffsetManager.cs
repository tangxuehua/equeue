using System;
using System.Collections.Concurrent;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Scheduling;

namespace EQueue.Broker
{
    public class InMemoryOffsetManager : IOffsetManager
    {
        private ConcurrentDictionary<string, ConcurrentDictionary<string, long>> _dict = new ConcurrentDictionary<string, ConcurrentDictionary<string, long>>();
        private readonly IScheduleService _scheduleService;
        private int _printQueueOffsetTaskId;
        private readonly ILogger _logger;

        public InMemoryOffsetManager()
        {
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void Recover() { }
        public void Start()
        {
            _printQueueOffsetTaskId = _scheduleService.ScheduleTask(PrintQueueOffset, 5000, 5000);
        }
        public void Shutdown()
        {
            _scheduleService.ShutdownTask(_printQueueOffsetTaskId);
        }

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
        private void PrintQueueOffset()
        {
            foreach (var entry1 in _dict)
            {
                _logger.DebugFormat("Queue consume offset statistic info of group [{0}]:", entry1.Key);
                foreach (var entry2 in entry1.Value)
                {
                    var items = entry2.Key.Split(new string[] { "-" }, StringSplitOptions.None);
                    _logger.DebugFormat("[topic:{0},queue{1}], consume offset:{2}", items[0], items[1], entry2.Value);
                }
            }
        }
    }
}
