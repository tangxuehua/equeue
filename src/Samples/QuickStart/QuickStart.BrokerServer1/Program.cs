using System;
using System.Configuration;
using System.Net;
using ECommon.Configurations;
using ECommon.Socketing;
using EQueue.Broker;
using EQueue.Configurations;
using EQueue.Utils;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace QuickStart.BrokerServer1
{
    class Program
    {
        static void Main(string[] args)
        {
            InitializeEQueue();
            var setting = new BrokerSetting(
                bool.Parse(ConfigurationManager.AppSettings["isMemoryMode"]),
                ConfigurationManager.AppSettings["fileStoreRootPath"],
                chunkCacheMaxPercent: 95,
                chunkFlushInterval: int.Parse(ConfigurationManager.AppSettings["flushInterval"]),
                messageChunkDataSize: int.Parse(ConfigurationManager.AppSettings["chunkSize"]) * 1024 * 1024,
                chunkWriteBuffer: int.Parse(ConfigurationManager.AppSettings["chunkWriteBuffer"]) * 1024,
                enableCache: bool.Parse(ConfigurationManager.AppSettings["enableCache"]),
                chunkCacheMinPercent: int.Parse(ConfigurationManager.AppSettings["chunkCacheMinPercent"]),
                syncFlush: bool.Parse(ConfigurationManager.AppSettings["syncFlush"]),
                messageChunkLocalCacheSize: 30 * 10000,
                queueChunkLocalCacheSize: 10000)
            {
                NotifyWhenMessageArrived = bool.Parse(ConfigurationManager.AppSettings["notifyWhenMessageArrived"]),
                MessageWriteQueueThreshold = int.Parse(ConfigurationManager.AppSettings["messageWriteQueueThreshold"])
            };
            setting.BrokerInfo.BrokerName = ConfigurationManager.AppSettings["brokerName"];
            setting.BrokerInfo.GroupName = ConfigurationManager.AppSettings["groupName"];
            setting.BrokerInfo.ProducerAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), int.Parse(ConfigurationManager.AppSettings["producerPort"])).ToAddress();
            setting.BrokerInfo.ConsumerAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), int.Parse(ConfigurationManager.AppSettings["consumerPort"])).ToAddress();
            setting.BrokerInfo.AdminAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), int.Parse(ConfigurationManager.AppSettings["adminPort"])).ToAddress();
            BrokerController.Create(setting).Start();
            Console.ReadLine();
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
                .RegisterEQueueComponents()
                .UseDeleteMessageByCountStrategy(10);
        }
    }
}
