using System;
using System.Collections.Concurrent;
using ECommon.IoC;
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
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
        }

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
            queueOffsetDict.AddOrUpdate(key, offset, (currentKey, oldOffset) =>
            {
                return offset > oldOffset ? offset : oldOffset;
            });
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
        private void PrintQueueOffset()
        {
            foreach (var entry1 in _dict)
            {
                _logger.InfoFormat("Group [{0}] queue offset info:", entry1.Key);
                foreach (var entry2 in entry1.Value)
                {
                    var items = entry2.Key.Split(new string[] { "-" }, StringSplitOptions.None);
                    _logger.InfoFormat("    [Topic:{0},queue{1}] offset:{2}", items[0], items[1], entry2.Value);
                }
            }
        }
    }
}
