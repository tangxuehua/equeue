using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Scheduling;

namespace EQueue.Broker
{
    public class DefaultChunkStatisticService : IChunkStatisticService
    {
        class BytesInfo
        {
            public long PreviousBytes;
            public long CurrentBytes;

            public long UpgradeBytes()
            {
                var incrementBytes = CurrentBytes - PreviousBytes;
                PreviousBytes = CurrentBytes;
                return incrementBytes;
            }
        }
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
        private ConcurrentDictionary<int, BytesInfo> _bytesWriteDict;
        private ConcurrentDictionary<int, CountInfo> _fileReadDict;
        private ConcurrentDictionary<int, CountInfo> _unmanagedReadDict;
        private ConcurrentDictionary<int, CountInfo> _cachedReadDict;

        public DefaultChunkStatisticService(IMessageStore messageStore, IScheduleService scheduleService, ILoggerFactory loggerFactory)
        {
            _messageStore = messageStore;
            _scheduleService = scheduleService;
            _logger = loggerFactory.Create("ChunkStatistic");
            _bytesWriteDict = new ConcurrentDictionary<int, BytesInfo>();
            _fileReadDict = new ConcurrentDictionary<int, CountInfo>();
            _unmanagedReadDict = new ConcurrentDictionary<int, CountInfo>();
            _cachedReadDict = new ConcurrentDictionary<int, CountInfo>();
        }

        public void AddWriteBytes(int chunkNum, int byteCount)
        {
            _bytesWriteDict.AddOrUpdate(chunkNum, GetDefaultBytesInfo, (chunkNumber, current) => UpdateBytesInfo(chunkNumber, current, byteCount));
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
            if (!BrokerController.Instance.Setting.IsMessageStoreMemoryMode)
            {
                _scheduleService.StartTask("LogChunkStatisticStatus", LogChunkStatisticStatus, 1000, 1000);
            }
        }
        public void Shutdown()
        {
            if (!BrokerController.Instance.Setting.IsMessageStoreMemoryMode)
            {
                _scheduleService.StopTask("LogChunkStatisticStatus");
            }
        }

        private CountInfo GetDefaultCountInfo(int chunkNum)
        {
            return new CountInfo { CurrentCount = 1 };
        }
        private CountInfo UpdateCountInfo(int chunkNum, CountInfo countInfo)
        {
            Interlocked.Increment(ref countInfo.CurrentCount);
            return countInfo;
        }
        private BytesInfo GetDefaultBytesInfo(int chunkNum)
        {
            return new BytesInfo();
        }
        private BytesInfo UpdateBytesInfo(int chunkNum, BytesInfo bytesInfo, int bytesAdd)
        {
            Interlocked.Add(ref bytesInfo.CurrentBytes, bytesAdd);
            return bytesInfo;
        }

        private void LogChunkStatisticStatus()
        {
            if (_logger.IsDebugEnabled)
            {
                var bytesWriteStatus = UpdateWriteStatus(_bytesWriteDict);
                var unmanagedReadStatus = UpdateReadStatus(_unmanagedReadDict);
                var fileReadStatus = UpdateReadStatus(_fileReadDict);
                var cachedReadStatus = UpdateReadStatus(_cachedReadDict);
                _logger.DebugFormat("maxChunk:#{0}, write:{1}, unmanagedCacheRead:{2}, localCacheRead:{3}, fileRead:{4}", _messageStore.MaxChunkNum, bytesWriteStatus, unmanagedReadStatus, cachedReadStatus, fileReadStatus);
            }
        }
        private string UpdateWriteStatus(ConcurrentDictionary<int, BytesInfo> dict)
        {
            var list = new List<string>();
            var toRemoveKeys = new List<int>();

            foreach (var entry in dict)
            {
                var chunkNum = entry.Key;
                var throughput = entry.Value.UpgradeBytes() / 1024;
                if (throughput > 0)
                {
                    list.Add(string.Format("[chunk:#{0},bytes:{1}KB]", chunkNum, throughput));
                }
                else
                {
                    toRemoveKeys.Add(chunkNum);
                }
            }

            foreach (var key in toRemoveKeys)
            {
                _bytesWriteDict.Remove(key);
            }

            return list.Count == 0 ? "[]" : string.Join(",", list);
        }
        private string UpdateReadStatus(ConcurrentDictionary<int, CountInfo> dict)
        {
            var list = new List<string>();

            foreach (var entry in dict)
            {
                var chunkNum = entry.Key;
                var throughput = entry.Value.UpgradeCount();
                if (throughput > 0)
                {
                    list.Add(string.Format("[chunk:#{0},count:{1}]", chunkNum, throughput));
                }
            }

            return list.Count == 0 ? "[]" : string.Join(",", list);
        }
    }
}
