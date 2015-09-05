using System;
using System.IO;
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
                .UseFileMessageStore(CreateDbConfig(@"d:\chunkdb", false));
        }
        static TFChunkDbConfig CreateDbConfig(string dbPath, bool inMemDb)
        {
            ICheckpoint writerCheckpoint;
            var name = "writer";
            if (inMemDb)
            {
                writerCheckpoint = new InMemoryCheckpoint(name);
            }
            else
            {
                writerCheckpoint = new MemoryMappedFileCheckpoint(Path.Combine(dbPath, name + ".chk"), name, cached: true);
            }

            var cache = Consts.ChunksCacheCount * (long)(Consts.ChunkSize + ChunkHeader.Size + ChunkFooter.Size);
            var config = new TFChunkDbConfig(dbPath,
                new VersionedPatternFileNamingStrategy(dbPath, "chunk-"),
                Consts.ChunkSize,
                cache,
                Consts.FlushChunkIntervalMilliseconds,
                writerCheckpoint,
                inMemDb);

            return config;
        }
    }
}
