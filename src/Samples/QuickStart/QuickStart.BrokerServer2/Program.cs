using System;
using System.Collections.Generic;
using System.Configuration;
using System.Net;
using ECommon.Configurations;
using ECommon.Extensions;
using ECommon.Socketing;
using EQueue.Broker;
using EQueue.Configurations;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace QuickStart.BrokerServer2
{
    class Program
    {
        static void Main(string[] args)
        {
            InitializeEQueue();

            var address = ConfigurationManager.AppSettings["nameServerAddress"];
            var nameServerAddress = string.IsNullOrEmpty(address) ? SocketUtils.GetLocalIPV4() : IPAddress.Parse(address);
            var setting = new BrokerSetting(
                isMessageStoreMemoryMode: bool.Parse(ConfigurationManager.AppSettings["isMemoryMode"]),
                chunkFileStoreRootPath: ConfigurationManager.AppSettings["fileStoreRootPath"],
                chunkFlushInterval: int.Parse(ConfigurationManager.AppSettings["flushInterval"]),
                chunkCacheMaxCount: int.Parse(ConfigurationManager.AppSettings["chunkCacheMaxCount"]),
                chunkCacheMinCount: int.Parse(ConfigurationManager.AppSettings["chunkCacheMinCount"]),
                messageChunkDataSize: int.Parse(ConfigurationManager.AppSettings["chunkSize"]) * 1024 * 1024,
                chunkWriteBuffer: int.Parse(ConfigurationManager.AppSettings["chunkWriteBuffer"]) * 1024,
                enableCache: bool.Parse(ConfigurationManager.AppSettings["enableCache"]),
                syncFlush: bool.Parse(ConfigurationManager.AppSettings["syncFlush"]),
                messageChunkLocalCacheSize: 30 * 10000,
                queueChunkLocalCacheSize: 10000)
            {
                NotifyWhenMessageArrived = bool.Parse(ConfigurationManager.AppSettings["notifyWhenMessageArrived"]),
                MessageWriteQueueThreshold = int.Parse(ConfigurationManager.AppSettings["messageWriteQueueThreshold"]),
                DeleteMessageIgnoreUnConsumed = bool.Parse(ConfigurationManager.AppSettings["deleteMessageIgnoreUnConsumed"])
            };
            setting.NameServerList = new List<IPEndPoint> { new IPEndPoint(nameServerAddress, 9493) };
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
                .UseDeleteMessageByCountStrategy(5)
                .BuildContainer();
        }
    }
}
