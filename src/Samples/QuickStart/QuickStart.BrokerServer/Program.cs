using System;
using ECommon.Autofac;
using ECommon.JsonNet;
using ECommon.Log4Net;
using EQueue.Broker;
using EQueue.Broker.Storage;
using EQueue.Configurations;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace QuickStart.BrokerServer
{
    class Program
    {
        static void Main(string[] args)
        {
            InitializeEQueue();
            BrokerController.Create(new BrokerSetting { NotifyWhenMessageArrived = false }).Start();
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
                .UseFileMessageStore(CreateMessageChunkManagerConfig());
        }
        static TFChunkManagerConfig CreateMessageChunkManagerConfig()
        {
            return new TFChunkManagerConfig(@"d:\chunkdb", new DefaultFileNamingStrategy("chunk-"),
                Consts.MaxChunksCount,
                Consts.ChunkDataSize,
                0,
                0,
                Consts.FlushChunkIntervalMilliseconds,
                Consts.TFChunkReaderCount);
        }
    }
}
