using Microsoft.VisualBasic.Devices;

namespace EQueue.Broker.Storage
{
    public class ChunkUtil
    {
        public struct ChunkApplyMemoryInfo
        {
            public ulong PhysicalMemoryMB;
            public ulong UsedMemoryPercent;
            public ulong UsedMemoryMB;
            public ulong MaxAllowUseMemoryMB;
            public ulong ChunkSizeMB;
            public long RemainingMemoryMB;
        }
        public static bool IsMemoryEnoughToCacheChunk(ulong chunkSize, uint chunkCacheMaxPercent, out ChunkApplyMemoryInfo applyMemoryInfo)
        {
            applyMemoryInfo = new ChunkApplyMemoryInfo();

            uint bytesPerMB = 1024 * 1024;
            var computerInfo = new ComputerInfo();
            var usedMemory = computerInfo.TotalPhysicalMemory - computerInfo.AvailablePhysicalMemory;

            applyMemoryInfo.PhysicalMemoryMB = computerInfo.TotalPhysicalMemory / bytesPerMB;
            applyMemoryInfo.UsedMemoryMB = usedMemory / bytesPerMB;
            applyMemoryInfo.UsedMemoryPercent = applyMemoryInfo.UsedMemoryMB * 100 / applyMemoryInfo.PhysicalMemoryMB;
            applyMemoryInfo.ChunkSizeMB = chunkSize / bytesPerMB;
            applyMemoryInfo.MaxAllowUseMemoryMB = applyMemoryInfo.PhysicalMemoryMB * chunkCacheMaxPercent / 100;
            applyMemoryInfo.RemainingMemoryMB = (long)applyMemoryInfo.MaxAllowUseMemoryMB - (long)applyMemoryInfo.UsedMemoryMB;
            return applyMemoryInfo.UsedMemoryMB + applyMemoryInfo.ChunkSizeMB <= applyMemoryInfo.MaxAllowUseMemoryMB;
        }
    }
}
