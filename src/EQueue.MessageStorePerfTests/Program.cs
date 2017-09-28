using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ECommon.Components;
using ECommon.Configurations;
using ECommon.Utilities;
using EQueue.Broker;
using EQueue.Configurations;
using EQueue.Protocols;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace EQueue.MessageStorePerfTests
{
    class Program
    {
        static IMessageStore _messageStore;
        static IPerformanceService _performanceService;

        static void Main(string[] args)
        {
            InitializeEQueue();
            WriteMessagePerfTest();
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
                .BuildContainer();

            BrokerController.Create(new BrokerSetting(false, ConfigurationManager.AppSettings["fileStoreRootPath"], enableCache: false, syncFlush: bool.Parse(ConfigurationManager.AppSettings["syncFlush"])));

            _messageStore = ObjectContainer.Resolve<IMessageStore>();
            _performanceService = ObjectContainer.Resolve<IPerformanceService>();
            _performanceService.Initialize("StoreMessage").Start();
            _messageStore.Load();
            _messageStore.Start();
        }
        static void WriteMessagePerfTest()
        {
            var threadCount = int.Parse(ConfigurationManager.AppSettings["concurrentThreadCount"]);                         //并行写消息的线程数
            var messageSize = int.Parse(ConfigurationManager.AppSettings["messageSize"]);                                   //消息大小，字节为单位
            var messageCount = int.Parse(ConfigurationManager.AppSettings["messageCount"]);                                 //总共要写入的消息数
            var batchSize = int.Parse(ConfigurationManager.AppSettings["batchSize"]);                                       //批量持久化大小
            var payload = new byte[messageSize];
            var messages = new List<Message>();
            var topic = "topic1";
            var queue = new Queue(topic, 1);
            var count = 0L;

            for (var i = 0; i < batchSize; i++)
            {
                messages.Add(new Message(topic, 100, payload));
            }
            for (var i = 0; i < threadCount; i++)
            {
                Task.Factory.StartNew(() =>
                {
                    while (true)
                    {
                        var current = Interlocked.Increment(ref count);
                        if (current > messageCount)
                        {
                            break;
                        }
                        foreach (var message in messages)
                        {
                            message.CreatedTime = DateTime.Now;
                        }
                        if (batchSize == 1)
                        {
                            _messageStore.StoreMessageAsync(queue, messages.First(), (x, y) =>
                            {
                                _performanceService.IncrementKeyCount("default", (DateTime.Now - x.CreatedTime).TotalMilliseconds);
                            }, null, null);
                        }
                        else
                        {
                            _messageStore.BatchStoreMessageAsync(queue, messages, (x, y) =>
                            {
                                foreach (var record in x.Records)
                                {
                                    _performanceService.IncrementKeyCount("default", (DateTime.Now - record.CreatedTime).TotalMilliseconds);
                                }
                            }, null, null);
                        }
                    }
                });
            }
        }
    }
}
