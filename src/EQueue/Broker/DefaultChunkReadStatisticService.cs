using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Scheduling;

namespace EQueue.Broker
{
    public class DefaultChunkReadStatisticService : IChunkReadStatisticService
    {
        class CountInfo
        {
            public long PreviousCount;
            public long CurrentCount;

            public long UpgradeCount()
            {
                var incrementCount = CurrentCount - PreviousCount;
                PreviousCount = CurrentCount;
                return incrementCount;
            }
        }
        private readonly ILogger _logger;
        private readonly IMessageStore _messageStore;
        private readonly IScheduleService _scheduleService;
        private ConcurrentDictionary<int, CountInfo> _fileReadDict;
        private ConcurrentDictionary<int, CountInfo> _unmanagedReadDict;
        private ConcurrentDictionary<int, CountInfo> _cachedReadDict;

        public DefaultChunkReadStatisticService(IMessageStore messageStore, IScheduleService scheduleService, ILoggerFactory loggerFactory)
        {
            _messageStore = messageStore;
            _scheduleService = scheduleService;
            _logger = loggerFactory.Create("ChunkRead");
            _fileReadDict = new ConcurrentDictionary<int, CountInfo>();
            _unmanagedReadDict = new ConcurrentDictionary<int, CountInfo>();
            _cachedReadDict = new ConcurrentDictionary<int, CountInfo>();
        }

        public void AddFileReadCount(int chunkNum)
        {
            _fileReadDict.AddOrUpdate(chunkNum, GetDefaultCountInfo, UpdateCountInfo);
        }
        public void AddUnmanagedReadCount(int chunkNum)
        {
            _unmanagedReadDict.AddOrUpdate(chunkNum, GetDefaultCountInfo, UpdateCountInfo);
        }
        public void AddCachedReadCount(int chunkNum)
        {
            _cachedReadDict.AddOrUpdate(chunkNum, GetDefaultCountInfo, UpdateCountInfo);
        }

        public void Start()
        {
            _scheduleService.StartTask("LogReadStatus", LogReadStatus, 1000, 1000);
        }
        public void Shutdown()
        {
            _scheduleService.StopTask("LogReadStatus");
        }

        private CountInfo GetDefaultCountInfo(int chunkNum)
        {
            return new CountInfo { CurrentCount = 1 };
        }
        private CountInfo UpdateCountInfo(int chunkNum, CountInfo countInfo)
        {
            countInfo.CurrentCount = Interlocked.Increment(ref countInfo.CurrentCount);
            return countInfo;
        }

        private void LogReadStatus()
        {
            if (_logger.IsDebugEnabled)
            {
                var unmanagedReadStatus = UpdateReadStatus(_unmanagedReadDict);
                var fileReadStatus = UpdateReadStatus(_fileReadDict);
                var cachedReadStatus = UpdateReadStatus(_cachedReadDict);
                _logger.DebugFormat("maxChunk:#{0}, unmanagedRead:{1}, cachedRead:{2}, fileRead:{3}", _messageStore.MaxChunkNum, unmanagedReadStatus, cachedReadStatus, fileReadStatus);
            }
        }
        private string UpdateReadStatus(ConcurrentDictionary<int, CountInfo> dict)
        {
            var list = new List<string>();
            var toRemoveKeys = new List<int>();

            foreach (var entry in dict)
            {
                var chunkNum = entry.Key;
                var throughput = entry.Value.UpgradeCount();
                if (throughput > 0)
                {
                    list.Add(string.Format("[chunk:#{0},count:{1}]", chunkNum, throughput));
                }
                else
                {
                    toRemoveKeys.Add(chunkNum);
                }
            }

            foreach (var key in toRemoveKeys)
            {
                _fileReadDict.Remove(key);
            }

            return list.Count == 0 ? "[]" : string.Join(",", list);
        }
    }
}
