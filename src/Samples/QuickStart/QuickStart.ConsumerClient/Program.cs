using System;
using System.Configuration;
using System.Diagnostics;
using System.Threading;
using ECommon.Autofac;
using ECommon.Components;
using ECommon.JsonNet;
using ECommon.Log4Net;
using ECommon.Logging;
using EQueue.Broker.Storage;
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
                new Consumer("Consumer@" + i.ToString(), "SampleGroup", consumerSetting)
                    .Subscribe("topic1")
                    .SetMessageHandler(messageHandler)
                    .Start();
            }
        }

        class MessageHandler : IMessageHandler
        {
            private long _handledCount;
            private Stopwatch _watch;

            public void Handle(QueueMessage message, IMessageContext context)
            {
                var currentCount = Interlocked.Increment(ref _handledCount);
                if (currentCount == 1)
                {
                    _watch = Stopwatch.StartNew();
                }
                if (currentCount % 10000 == 0)
                {
                    _logger.InfoFormat("Total handled {0} messages, timeSpent: {1}ms, throughput: {2}/s", currentCount, _watch.ElapsedMilliseconds, currentCount * 1000 / _watch.ElapsedMilliseconds);
                }

                context.OnMessageHandled(message);
            }
        }
    }
}
