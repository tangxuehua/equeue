using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using ECommon.Autofac;
using ECommon.Configurations;
using ECommon.Components;
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
            var consumerSetting = new ConsumerSetting { HeartbeatBrokerInterval = 1000, UpdateTopicQueueCountInterval = 1000, RebalanceInterval = 1000 };
            var consumer1 = new Consumer("Consumer1", "group1", consumerSetting).Subscribe("SampleTopic").Start(messageHandler);
            var consumer2 = new Consumer("Consumer2", "group1", consumerSetting).Subscribe("SampleTopic").Start(messageHandler);

            _logger.Info("Start consumer load balance, please wait for a moment.");
            var scheduleService = ObjectContainer.Resolve<IScheduleService>();
            var waitHandle = new ManualResetEvent(false);
            var taskId = scheduleService.ScheduleTask(() =>
            {
                var c1AllocatedQueueIds = consumer1.GetCurrentQueues().Select(x => x.QueueId);
                var c2AllocatedQueueIds = consumer2.GetCurrentQueues().Select(x => x.QueueId);
                if (c1AllocatedQueueIds.Count() == 2 && c2AllocatedQueueIds.Count() == 2)
                {
                    _logger.Info(string.Format("Consumer load balance finished. Queue allocation result: c1:{0}, c2:{1}",
                        string.Join(",", c1AllocatedQueueIds),
                        string.Join(",", c2AllocatedQueueIds)));
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

        static ConcurrentDictionary<long, long> _handledMessageDict = new ConcurrentDictionary<long, long>();
        class MessageHandler : IMessageHandler
        {
            private int _handledCount;
            private Stopwatch _watch;

            public void Handle(QueueMessage message, IMessageContext context)
            {
                if (_handledMessageDict.TryAdd(message.MessageOffset, message.MessageOffset))
                {
                    var count = Interlocked.Increment(ref _handledCount);
                    if (count == 1)
                    {
                        _watch = Stopwatch.StartNew();
                    }
                    else if (count % 1000 == 0 || (count - 4) % 50000 == 0)
                    {
                        _logger.InfoFormat("Total handled {0} messages, time spent:{1}", count, _watch.ElapsedMilliseconds);
                    }
                    context.OnMessageHandled(message);
                }
            }
        }
    }
}
