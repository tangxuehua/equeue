using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using ECommon.Autofac;
using ECommon.Configurations;
using ECommon.IoC;
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
            var consumer1 = new Consumer("Consumer1", "group1").Subscribe("SampleTopic").Start(messageHandler);
            var consumer2 = new Consumer("Consumer2", "group1").Subscribe("SampleTopic").Start(messageHandler);
            var consumer3 = new Consumer("Consumer3", "group1").Subscribe("SampleTopic").Start(messageHandler);
            var consumer4 = new Consumer("Consumer4", "group1").Subscribe("SampleTopic").Start(messageHandler);

            _logger.Info("Start consumer load balance, please wait for a moment.");
            var scheduleService = ObjectContainer.Resolve<IScheduleService>();
            var waitHandle = new ManualResetEvent(false);
            var taskId = scheduleService.ScheduleTask(() =>
            {
                var c1AllocatedQueueIds = consumer1.GetCurrentQueues().Select(x => x.QueueId);
                var c2AllocatedQueueIds = consumer2.GetCurrentQueues().Select(x => x.QueueId);
                var c3AllocatedQueueIds = consumer3.GetCurrentQueues().Select(x => x.QueueId);
                var c4AllocatedQueueIds = consumer4.GetCurrentQueues().Select(x => x.QueueId);
                if (c1AllocatedQueueIds.Count() == 1 && c2AllocatedQueueIds.Count() == 1 && c3AllocatedQueueIds.Count() == 1 && c4AllocatedQueueIds.Count() == 1)
                {
                    _logger.Info(string.Format("Consumer load balance finished. Queue allocation result: c1:{0}, c2:{1}, c3:{2}, c4:{3}",
                        string.Join(",", c1AllocatedQueueIds),
                        string.Join(",", c2AllocatedQueueIds),
                        string.Join(",", c3AllocatedQueueIds),
                        string.Join(",", c4AllocatedQueueIds)));
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
