using System;
using System.Configuration;
using System.Diagnostics;
using System.Threading;
using ECommon.Autofac;
using ECommon.Components;
using ECommon.JsonNet;
using ECommon.Log4Net;
using ECommon.Logging;
using ECommon.Scheduling;
using EQueue.Clients.Consumers;
using EQueue.Configurations;
using EQueue.Protocols;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace QuickStart.ConsumerClient
{
    class Program
    {
        static void Main(string[] args)
        {
            InitializeEQueue();
            Console.ReadLine();
        }

        static ILogger _logger;
        static void InitializeEQueue()
        {
            ECommonConfiguration
                .Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4Net()
                .UseJsonNet()
                .RegisterUnhandledExceptionHandler()
                .RegisterEQueueComponents();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create("Program");

            var clientCount = int.Parse(ConfigurationManager.AppSettings["ClientCount"]);
            var consumerSetting = new ConsumerSetting
            {
                HeartbeatBrokerInterval = 1000,
                UpdateTopicQueueCountInterval = 1000,
                RebalanceInterval = 1000,
                ConsumeFromWhere = ConsumeFromWhere.FirstOffset
            };
            var messageHandler = new MessageHandler();
            for (var i = 1; i <= clientCount; i++)
            {
                new Consumer("SampleGroup", consumerSetting)
                    .Subscribe("topic1")
                    .SetMessageHandler(messageHandler)
                    .Start();
            }
        }

        class MessageHandler : IMessageHandler
        {
            private long _previusHandledCount;
            private long _handledCount;
            private IScheduleService _scheduleService;

            public MessageHandler()
            {
                _scheduleService = ObjectContainer.Resolve<IScheduleService>();
                _scheduleService.StartTask("Program.PrintThroughput", PrintThroughput, 0, 1000);
            }

            public void Handle(QueueMessage message, IMessageContext context)
            {
                Interlocked.Increment(ref _handledCount);
                context.OnMessageHandled(message);
            }

            private void PrintThroughput()
            {
                var totalHandledCount = _handledCount;
                var totalCountOfCurrentPeriod = totalHandledCount - _previusHandledCount;
                _previusHandledCount = totalHandledCount;

                _logger.InfoFormat("currentTime: {0}, totalReceived: {1}, throughput: {2}/s", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"), totalHandledCount, totalCountOfCurrentPeriod);
            }
        }
    }
}
