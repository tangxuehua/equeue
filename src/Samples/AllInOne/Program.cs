using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ECommon.Autofac;
using ECommon.Configurations;
using ECommon.IoC;
using ECommon.JsonNet;
using ECommon.Log4Net;
using ECommon.Scheduling;
using EQueue.Broker;
using EQueue.Clients.Consumers;
using EQueue.Clients.Producers;
using EQueue.Configurations;
using EQueue.Protocols;

namespace AllInOne
{
    class Program
    {
        static void Main(string[] args)
        {
            InitializeEQueue();

            StartBroker();
            StartConsumers();
            StartProducer();

            Console.ReadLine();
        }

        static void InitializeEQueue()
        {
            Configuration
                .Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4Net()
                .UseJsonNet()
                .RegisterEQueueComponents();
        }
        static void StartBroker()
        {
            var setting = BrokerSetting.Default;
            setting.NotifyWhenMessageArrived = false;
            new BrokerController(setting).Initialize().Start();
        }
        static void StartConsumers()
        {
            var messageHandler = new MessageHandler();

            //Start four consumers.
            var consumer1 = new Consumer("Consumer1", "group1", messageHandler).Subscribe("SampleTopic").Start();
            var consumer2 = new Consumer("Consumer2", "group1", messageHandler).Subscribe("SampleTopic").Start();
            var consumer3 = new Consumer("Consumer3", "group1", messageHandler).Subscribe("SampleTopic").Start();
            var consumer4 = new Consumer("Consumer4", "group1", messageHandler).Subscribe("SampleTopic").Start();

            //Below to wait for consumer balance.
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
                    Console.WriteLine(string.Format("Consumer message queue allocation result: c1:{0}, c2:{1}, c3:{2}, c4:{3}",
                        string.Join(",", c1AllocatedQueueIds),
                        string.Join(",", c2AllocatedQueueIds),
                        string.Join(",", c3AllocatedQueueIds),
                        string.Join(",", c4AllocatedQueueIds)));
                    waitHandle.Set();
                }
            }, 1000, 1000);

            waitHandle.WaitOne();
            scheduleService.ShutdownTask(taskId);
        }
        static void StartProducer()
        {
            var producer = new Producer(ProducerSetting.Default).Start();
            var total = 1000;
            var parallelCount = 10;
            var finished = 0;
            var messageIndex = 0;
            var watch = Stopwatch.StartNew();

            var action = new Action(() =>
            {
                for (var index = 1; index <= total; index++)
                {
                    var message = "message" + Interlocked.Increment(ref messageIndex);
                    producer.SendAsync(new Message("SampleTopic", Encoding.UTF8.GetBytes(message)), index).ContinueWith(sendTask =>
                    {
                        var finishedCount = Interlocked.Increment(ref finished);
                        if (finishedCount % 1000 == 0)
                        {
                            Console.WriteLine(string.Format("Sent {0} messages, time spent:{1}", finishedCount, watch.ElapsedMilliseconds));
                        }
                    });
                }
            });

            var actions = new List<Action>();
            for (var index = 0; index < parallelCount; index++)
            {
                actions.Add(action);
            }

            Parallel.Invoke(actions.ToArray());
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
                    Console.WriteLine("Total handled {0} messages, time spent:{1}", count, _watch.ElapsedMilliseconds);
                }
                context.OnMessageHandled(message);
            }
        }
    }
}
