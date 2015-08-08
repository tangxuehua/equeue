using System;
using System.Diagnostics;
using System.Threading;
using ECommon.Autofac;
using ECommon.Components;
using ECommon.Configurations;
using ECommon.JsonNet;
using ECommon.Log4Net;
using ECommon.Logging;
using ECommon.Utilities;
using EQueue.Clients.Consumers;
using EQueue.Configurations;
using EQueue.Protocols;

namespace QuickStart.ConsumerClient
{
    class Program
    {
        static void Main(string[] args)
        {
            InitializeEQueue();

            var messageHandler = new MessageHandler();
            var consumerSetting = new ConsumerSetting
            {
                HeartbeatBrokerInterval = 1000,
                UpdateTopicQueueCountInterval = 1000,
                RebalanceInterval = 1000,
                ConsumeFromWhere = ConsumeFromWhere.FirstOffset
            };
            var consumer = new Consumer("SampleConsumer@" + ObjectId.GenerateNewStringId(), "SampleGroup", consumerSetting).Subscribe("SampleTopic").SetMessageHandler(messageHandler).Start();
            Console.ReadLine();
        }

        static ILogger _logger;
        static void InitializeEQueue()
        {
            Configuration
                .Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4Net()
                .UseJsonNet()
                .RegisterEQueueComponents();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create("Program");
        }

        class MessageHandler : IMessageHandler
        {
            private int _handledCount;
            private Stopwatch _watch;
            private int _previousHandledCount;
            private long _previousElapsedMilliseconds;

            public void Handle(QueueMessage message, IMessageContext context)
            {
                var currentCount = Interlocked.Increment(ref _handledCount);
                if (currentCount == 1)
                {
                    _watch = Stopwatch.StartNew();
                }
                if (currentCount % 10000 == 0)
                {
                    var currentElapsedMilliseconds = _watch.ElapsedMilliseconds;
                    _logger.InfoFormat("Total handled {0} messages, throughput: {1}, latency: {2}ms",
                        currentCount,
                        (currentCount - _previousHandledCount) * 1000 / (currentElapsedMilliseconds - _previousElapsedMilliseconds),
                        (DateTime.Now - message.CreatedTime).TotalMilliseconds);
                    _previousHandledCount = currentCount;
                    _previousElapsedMilliseconds = currentElapsedMilliseconds;
                }

                context.OnMessageHandled(message);
            }
        }
    }
}
