using System;
using System.Configuration;
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
            private long _calculateCount = 0;
            private IScheduleService _scheduleService;
            private ILogger _logger;
            private ILogger _throughputLogger;

            public MessageHandler()
            {
                _scheduleService = ObjectContainer.Resolve<IScheduleService>();
                _scheduleService.StartTask("Program.PrintThroughput", PrintThroughput, 1000, 1000);
                _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(typeof(Program).Name);
                _throughputLogger = ObjectContainer.Resolve<ILoggerFactory>().Create("throughput");
            }

            public void Handle(QueueMessage message, IMessageContext context)
            {
                Interlocked.Increment(ref _handledCount);
                context.OnMessageHandled(message);
            }

            private void PrintThroughput()
            {
                var totalHandledCount = _handledCount;
                var throughput = totalHandledCount - _previusHandledCount;
                _previusHandledCount = totalHandledCount;
                if (throughput > 0)
                {
                    _calculateCount++;
                }

                var average = 0L;
                if (_calculateCount > 0)
                {
                    average = totalHandledCount / _calculateCount;
                }

                _logger.InfoFormat("totalReceived: {0}, throughput: {1}/s, average: {2}", totalHandledCount, throughput, average);
                _throughputLogger.Info(throughput);
            }
        }
    }
}
