using System;
using System.Configuration;
using System.Text;
using ECommon.Autofac;
using ECommon.Components;
using ECommon.JsonNet;
using ECommon.Log4Net;
using ECommon.Scheduling;
using EQueue.Broker;
using EQueue.Configurations;
using EQueue.Utils;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace QuickStart.BrokerServer
{
    class Program
    {
        static void Main(string[] args)
        {
            InitializeEQueue();
            BrokerController.Create(new BrokerSetting().SetMessageChunkConfig(ConfigurationManager.AppSettings["fileStoreRootPath"])).Start();
            var scheduleService = ObjectContainer.Resolve<IScheduleService>();
            scheduleService.StartTask("PrintMemInfo", () =>
            {
                var memoryInfo = MemoryInfoUtil.GetMemInfo();

                var sb = new StringBuilder();

                //*%的内存正在使用
                sb.Append("\r\n" + memoryInfo.dwMemoryLoad.ToString() + "% of the memory is being used " + "\r\n");
                //总共的物理内存
                sb.Append("Physical memory total :" + Utility.ConvertBytes(memoryInfo.dwTotalPhys.ToString(), 2) + "MB" + "\r\n");
                //可使用的物理内存
                sb.Append("Use of physical memory :" + Utility.ConvertBytes(memoryInfo.dwAvailPhys.ToString(), 2) + "MB" + "\r\n");
                //交换文件总大小
                sb.Append("Total size of the swap file" + Utility.ConvertBytes(memoryInfo.dwTotalPageFile.ToString(), 2) + "MB" + "\r\n");
                //尚可交换文件大小为
                sb.Append("Can still swap file size :" + Utility.ConvertBytes(memoryInfo.dwAvailPageFile.ToString(), 2) + "MB" + "\r\n");
                //总虚拟内存
                sb.Append("The Total virtual memory :" + Utility.ConvertBytes(memoryInfo.dwTotalVirtual.ToString(), 2) + "MB" + "\r\n");
                //未用虚拟内存有
                sb.Append("Unused virtual memory :" + Utility.ConvertBytes(memoryInfo.dwAvailVirtual.ToString(), 2) + "MB" + "\r\n");

                Console.Write(sb.ToString());

            }, 1000, 1000);
            Console.ReadLine();
        }

        class Utility
        {
            public static decimal ConvertBytes(string b, int iteration)
            {
                long iter = 1;
                for (int i = 0; i < iteration; i++)
                {
                    iter *= 1024;
                }
                return Math.Round((Convert.ToDecimal(b)) / Convert.ToDecimal(iter), 2, MidpointRounding.AwayFromZero);
            }
        }

        static void InitializeEQueue()
        {
            var configuration = ECommonConfiguration
                .Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4Net()
                .UseJsonNet()
                .RegisterUnhandledExceptionHandler()
                .RegisterEQueueComponents();
        }
    }
}
