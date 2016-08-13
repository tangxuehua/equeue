using System;
using System.Configuration;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using ECommon.Components;
using ECommon.Configurations;
using ECommon.Logging;
using ECommon.Scheduling;
using EQueue.Broker;
using EQueue.Configurations;
using EQueue.Protocols;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace EQueue.MessageStorePerfTests
{
    class Program
    {
        static ILogger _logger;
        static IMessageStore _messageStore;
        static IScheduleService _scheduleService;
        static Stopwatch _watch;
        static long _currentCount = 0;
        static long _previousCount = 0;

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
                .RegisterEQueueComponents();

            BrokerController.Create(new BrokerSetting(false, ConfigurationManager.AppSettings["fileStoreRootPath"], enableCache: false));

            _messageStore = ObjectContainer.Resolve<IMessageStore>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(typeof(Program).Name);

            _messageStore.Load();
            _messageStore.Start();
        }
        static void WriteMessagePerfTest()
        {
            var threadCount = int.Parse(ConfigurationManager.AppSettings["concurrentThreadCount"]);                         //并行写消息的线程数
            var messageSize = int.Parse(ConfigurationManager.AppSettings["messageSize"]);                                   //消息大小，字节为单位
            var messageCount = int.Parse(ConfigurationManager.AppSettings["messageCount"]);                                 //总共要写入的消息数
            var payload = new byte[messageSize];
            var message = new Message("topic1", 100, payload);
            var queue = new Queue(message.Topic, 1);
            var count = 0L;

            _watch = Stopwatch.StartNew();
            StartPrintThroughputTask();

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
                        _messageStore.StoreMessageAsync(queue, message, (x, y) =>
                        {
                            Interlocked.Increment(ref _currentCount);
                        }, null);
                    }
                });
            }
        }
        static void StartPrintThroughputTask()
        {
            _scheduleService.StartTask("PrintThroughput", PrintThroughput, 1000, 1000);
        }
        static void PrintThroughput()
        {
            var currentCount = _currentCount;
            var throughput = currentCount - _previousCount;
            _previousCount = currentCount;
            var time = _watch.ElapsedMilliseconds;

            _logger.InfoFormat("Store message, totalCount: {0}, average throughput: {1}/s, current throughput: {2}/s", currentCount, currentCount * 1000 / time, throughput);
        }
    }
}
