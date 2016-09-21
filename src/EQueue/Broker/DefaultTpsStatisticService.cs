using System.Collections.Concurrent;
using System.Threading;
using ECommon.Logging;
using ECommon.Scheduling;

namespace EQueue.Broker
{
    public class DefaultTpsStatisticService : ITpsStatisticService
    {
        private const int ConsumeTpsStatInterval = 5;

        class CountInfo
        {
            public long PreviousCount;
            public long CurrentCount;
            public long Throughput;

            public void CalculateThroughput()
            {
                Throughput = CurrentCount - PreviousCount;
                PreviousCount = CurrentCount;
            }
        }
        private readonly IScheduleService _scheduleService;
        private ConcurrentDictionary<string, CountInfo> _sendTpsDict;
        private ConcurrentDictionary<string, CountInfo> _consumeTpsDict;

        public DefaultTpsStatisticService(IScheduleService scheduleService)
        {
            _scheduleService = scheduleService;
            _sendTpsDict = new ConcurrentDictionary<string, CountInfo>();
            _consumeTpsDict = new ConcurrentDictionary<string, CountInfo>();
        }

        public void AddTopicSendCount(string topic, int queueId)
        {
            _sendTpsDict.AddOrUpdate(string.Format("{0}_{1}", topic, queueId),
            x =>
            {
                return new CountInfo { CurrentCount = 1 };
            },
            (x, y) =>
            {
                Interlocked.Increment(ref y.CurrentCount);
                return y;
            });
        }
        public void UpdateTopicConsumeOffset(string topic, int queueId, string consumeGroup, long consumeOffset)
        {
            _consumeTpsDict.AddOrUpdate(string.Format("{0}_{1}_{2}", topic, queueId, consumeGroup),
            x =>
            {
                return new CountInfo { CurrentCount = consumeOffset };
            },
            (x, y) =>
            {
                y.CurrentCount = consumeOffset;
                return y;
            });
        }
        public long GetTopicSendThroughput(string topic, int queueId)
        {
            var key = string.Format("{0}_{1}", topic, queueId);
            CountInfo countInfo;
            if (_sendTpsDict.TryGetValue(key, out countInfo))
            {
                return countInfo.Throughput;
            }
            return 0L;
        }
        public long GetTopicConsumeThroughput(string topic, int queueId, string consumeGroup)
        {
            var key = string.Format("{0}_{1}_{2}", topic, queueId, consumeGroup);
            CountInfo countInfo;
            if (_consumeTpsDict.TryGetValue(key, out countInfo))
            {
                return countInfo.Throughput / ConsumeTpsStatInterval;
            }
            return 0L;
        }
        public long GetTotalSendThroughput()
        {
            long total = 0L;
            foreach (var countInfo in _sendTpsDict.Values)
            {
                total += countInfo.Throughput;
            }
            return total;
        }
        public long GetTotalConsumeThroughput()
        {
            long total = 0L;
            foreach (var countInfo in _consumeTpsDict.Values)
            {
                total += countInfo.Throughput;
            }
            return total / ConsumeTpsStatInterval;
        }

        public void Start()
        {
            _scheduleService.StartTask("CalculateSendThroughput", () =>
            {
                foreach (var entry in _sendTpsDict)
                {
                    entry.Value.CalculateThroughput();
                }
            }, 1000, 1000);
            _scheduleService.StartTask("CalculateConsumeThroughput", () =>
            {
                foreach (var entry in _consumeTpsDict)
                {
                    entry.Value.CalculateThroughput();
                }
            }, 1000, ConsumeTpsStatInterval * 1000);
        }
        public void Shutdown()
        {
            _scheduleService.StopTask("CalculateSendThroughput");
            _scheduleService.StopTask("CalculateConsumeThroughput");
        }
    }
}
