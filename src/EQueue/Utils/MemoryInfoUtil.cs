using System;
using System.Runtime.InteropServices;

namespace EQueue.Utils
{
    public class MemoryInfoUtil
    {
        [DllImport("kernel32")]
        private static extern void GlobalMemoryStatus(ref MemoryInfo memoryInfo);

        public static MemoryInfo GetMemInfo()
        {
            var memoryInfo = new MemoryInfo();
            GlobalMemoryStatus(ref memoryInfo);
            return memoryInfo;
        }

        public static decimal GetTotalPhysicalMemorySize()
        {
            var memoryInfo = GetMemInfo();
            return Math.Round((Convert.ToDecimal(memoryInfo.dwTotalPhys.ToString())) / Convert.ToDecimal(1024 * 1024), 2, MidpointRounding.AwayFromZero);
        }

        public static decimal GetUsedMemoryPercent()
        {
            return GetMemInfo().dwMemoryLoad;
        }
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct MemoryInfo
    {
        public uint dwLength;
        public uint dwMemoryLoad;
        public uint dwTotalPhys;
        public uint dwAvailPhys;
        public uint dwTotalPageFile;
        public uint dwAvailPageFile;
        public uint dwTotalVirtual;
        public uint dwAvailVirtual;
    }
}
