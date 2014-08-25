using System;
using System.Collections.Concurrent;
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
            var consumerSetting = new ConsumerSetting {
                SuspendPullRequestMilliseconds = 120000,
                PullRequestTimeoutMilliseconds = 180000,
                HeartbeatBrokerInterval = 1000,
                UpdateTopicQueueCountInterval = 1000,
                RebalanceInterval = 1000 };
            var consumer = new Consumer("Consumer1", "Group1", consumerSetting)
                .Subscribe("SampleTopic1")
                .Subscribe("SampleTopic2")
                .Subscribe("SampleTopic3")
                .Subscribe("SampleTopic4")
                .Subscribe("SampleTopic5")
                .Subscribe("SampleTopic6")
                .Subscribe("SampleTopic7")
                .Subscribe("SampleTopic8")
                .Subscribe("SampleTopic9")
                .Subscribe("SampleTopic10")
                .Subscribe("SampleTopic11")
                .Subscribe("SampleTopic12")
                .Subscribe("SampleTopic13")
                .Subscribe("SampleTopic14")
                .Subscribe("SampleTopic15")
                .Subscribe("SampleTopic16")
                .Subscribe("SampleTopic17")
                .Subscribe("SampleTopic18")
                .Subscribe("SampleTopic19")
                .Subscribe("SampleTopic20")
                .Subscribe("SampleTopic21")
                .Subscribe("SampleTopic22")
                .Subscribe("SampleTopic23")
                .Subscribe("SampleTopic24")
                .Subscribe("SampleTopic25")
                .Subscribe("SampleTopic26")
                .Subscribe("SampleTopic27")
                .Subscribe("SampleTopic28")
                .Subscribe("SampleTopic29")
                .Subscribe("SampleTopic30")
                .SetMessageHandler(messageHandler).Start();

            //_logger.Info("Start consumer load balance, please wait for a moment.");
            //var scheduleService = ObjectContainer.Resolve<IScheduleService>();
            //var waitHandle = new ManualResetEvent(false);
            //var taskId = scheduleService.ScheduleTask("WaitQueueAllocationComplete", () =>
            //{
            //    var allocatedQueueIds = consumer.GetCurrentQueues().Select(x => x.QueueId);
            //    if (allocatedQueueIds.Count() == 4)
            //    {
            //        _logger.InfoFormat("Consumer load balance completed, allocated queueIds:{0}", string.Join(",", allocatedQueueIds));
            //        waitHandle.Set();
            //    }
            //}, 1000, 1000);

            //waitHandle.WaitOne();
            //scheduleService.ShutdownTask(taskId);

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
                    else if (count % 1000 == 0)
                    {
                        _logger.InfoFormat("Total handled {0} messages, time spent:{1}", count, _watch.ElapsedMilliseconds);
                    }
                }
                context.OnMessageHandled(message);
            }
        }
    }
}
