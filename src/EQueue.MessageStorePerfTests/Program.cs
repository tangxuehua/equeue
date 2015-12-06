using System;
using System.Configuration;
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
        static long _currentCount = 0;
        static long _previousCount = 0;

        static void Main(string[] args)
        {
            InitializeEQueue();
            WriteMessagePerfTest();
            StartPrintThroughputTask();
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

            BrokerController.Create(new BrokerSetting(ConfigurationManager.AppSettings["fileStoreRootPath"], 1024 * 1024 * 1024, enableCache: false));

            _messageStore = ObjectContainer.Resolve<IMessageStore>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(typeof(Program).Name);

            _messageStore.Load();
            _messageStore.Start();
        }
        static void WriteMessagePerfTest()
        {
            var threadCount = int.Parse(ConfigurationManager.AppSettings["concurrentThreadCount"]);     //并行写消息的线程数
            var messageSize = int.Parse(ConfigurationManager.AppSettings["messageSize"]);               //消息大小，字节为单位
            var messageCount = int.Parse(ConfigurationManager.AppSettings["messageCount"]);             //总共要写入的消息数
            var payload = new byte[messageSize];
            var message = new Message("topic1", 100, payload);

            for (var i = 0; i < threadCount; i++)
            {
                Task.Factory.StartNew(() =>
                {
                    for (var j = 0; j < messageCount; j++)
                    {
                        _messageStore.StoreMessage(1, 1000, message);
                        Interlocked.Increment(ref _currentCount);
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

            _logger.InfoFormat("Store message, totalCount: {0}, throughput: {1}/s", currentCount, throughput);
        }
    }
}
