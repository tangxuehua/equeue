using System;
using System.Linq;
using System.Text;
using System.Threading;
using ECommon.Autofac;
using ECommon.Configurations;
using ECommon.IoC;
using ECommon.JsonNet;
using ECommon.Log4Net;
using ECommon.Scheduling;
using EQueue.Broker;
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

            var scheduleService = ObjectContainer.Resolve<IScheduleService>();
            scheduleService.ScheduleTask(() =>
            {
                Console.WriteLine(string.Format("Consumer message queue allocation. c1:{0}, c2:{1}, c3:{2}, c4:{3}",
                    string.Join(",", consumer1.GetCurrentQueues().Select(x => x.QueueId)),
                    string.Join(",", consumer2.GetCurrentQueues().Select(x => x.QueueId)),
                    string.Join(",", consumer3.GetCurrentQueues().Select(x => x.QueueId)),
                    string.Join(",", consumer4.GetCurrentQueues().Select(x => x.QueueId))));
            }, 5000, 5000);

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
    }

    class MessageHandler : IMessageHandler
    {
        private int _handledCount;

        public void Handle(QueueMessage message, IMessageContext context)
        {
            var count = Interlocked.Increment(ref _handledCount);
            if (count % 1000 == 0)
            {
                Console.WriteLine("Total handled {0} messages.", count);
            }
            context.OnMessageHandled(message);
        }
    }
}
