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
                ConsumeFromWhere = ConsumeFromWhere.FirstOffset
            };
            var messageHandler = new MessageHandler();
            for (var i = 1; i <= clientCount; i++)
            {
                new Consumer(ConfigurationManager.AppSettings["ConsumerGroup"], consumerSetting)
                    .Subscribe(ConfigurationManager.AppSettings["Topic"])
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
                _scheduleService.StartTask("Program.PrintThroughput", PrintThroughput, 1000, 1000);
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

                _logger.InfoFormat("totalReceived: {0}, throughput: {1}/s", totalHandledCount, totalCountOfCurrentPeriod);
            }
        }
    }
}
