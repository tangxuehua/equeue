using System;
using System.Linq;
using System.Text;
using System.Threading;
using EQueue;
using EQueue.Autofac;
using EQueue.Clients.Consumers;
using EQueue.Infrastructure.IoC;
using EQueue.Infrastructure.Scheduling;
using EQueue.JsonNet;
using EQueue.Log4Net;
using EQueue.Protocols;

namespace QuickStart.ConsumerClient
{
    class Program
    {
        static void Main(string[] args)
        {
            InitializeEQueue();

            var messageHandler = new MessageHandler();
            var consumer1 = new Consumer("consumer1", ConsumerSettings.Default, "group1", MessageModel.Clustering, messageHandler).Subscribe("SampleTopic").Start();
            var consumer2 = new Consumer("consumer2", ConsumerSettings.Default, "group1", MessageModel.Clustering, messageHandler).Subscribe("SampleTopic").Start();
            var consumer3 = new Consumer("consumer3", ConsumerSettings.Default, "group1", MessageModel.Clustering, messageHandler).Subscribe("SampleTopic").Start();
            var consumer4 = new Consumer("consumer4", ConsumerSettings.Default, "group1", MessageModel.Clustering, messageHandler).Subscribe("SampleTopic").Start();

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
                .UseLog4Net()
                .UseJsonNet()
                .RegisterFrameworkComponents();
        }
    }

    class MessageHandler : IMessageHandler
    {
        private int _handledCount;

        public void Handle(QueueMessage message)
        {
            var count = Interlocked.Increment(ref _handledCount);
            if (count % 1000 == 0)
            {
                Console.WriteLine("Total handled {0} messages.", count);
            }
        }
    }
}
