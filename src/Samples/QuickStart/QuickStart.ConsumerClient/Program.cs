using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using ECommon.Autofac;
using ECommon.Components;
using ECommon.Configurations;
using ECommon.JsonNet;
using ECommon.Log4Net;
using ECommon.Logging;
using ECommon.Scheduling;
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
            var consumer = new Consumer("Consumer1", "Group1", consumerSetting).Subscribe("SampleTopic").SetMessageHandler(messageHandler).Start();

            _logger.Info("Start consumer load balance, please wait for a moment.");
            var scheduleService = ObjectContainer.Resolve<IScheduleService>();
            var waitHandle = new ManualResetEvent(false);
            var taskId = scheduleService.ScheduleTask("WaitQueueAllocationComplete", () =>
            {
                var allocatedQueueIds = consumer.GetCurrentQueues().Select(x => x.QueueId);
                if (allocatedQueueIds.Count() == 4)
                {
                    _logger.InfoFormat("Consumer load balance completed, allocated queueIds:{0}", string.Join(",", allocatedQueueIds));
                    waitHandle.Set();
                }
            }, 1000, 1000);

            waitHandle.WaitOne();
            scheduleService.ShutdownTask(taskId);

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

            public void Handle(QueueMessage message, IMessageContext context)
            {
                var count = Interlocked.Increment(ref _handledCount);
                if (count == 1)
                {
                    _watch = Stopwatch.StartNew();
                }
                else if (count % 1000 == 0)
                {
                    _logger.InfoFormat("Total handled {0} messages, time spent:{1}", count, _watch.ElapsedMilliseconds);
                }

                context.OnMessageHandled(message);
            }
        }
    }
}
