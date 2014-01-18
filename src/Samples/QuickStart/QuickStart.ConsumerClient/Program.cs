using System;
using System.Linq;
using System.Text;
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

            var messageHandler1 = new MessageHandler();
            var consumer1 = new Consumer("consumer1", ConsumerSettings.Default, "group1", MessageModel.Clustering, messageHandler1);
            messageHandler1.SetConsumer(consumer1);
            consumer1.Subscribe("SampleTopic").Start();

            var messageHandler2 = new MessageHandler();
            var consumer2 = new Consumer("consumer2", ConsumerSettings.Default, "group1", MessageModel.Clustering, messageHandler2);
            messageHandler2.SetConsumer(consumer2);
            consumer2.Subscribe("SampleTopic").Start();

            var messageHandler3 = new MessageHandler();
            var consumer3 = new Consumer("consumer3", ConsumerSettings.Default, "group1", MessageModel.Clustering, messageHandler3);
            messageHandler3.SetConsumer(consumer3);
            consumer3.Subscribe("SampleTopic").Start();

            var messageHandler4 = new MessageHandler();
            var consumer4 = new Consumer("consumer4", ConsumerSettings.Default, "group1", MessageModel.Clustering, messageHandler4);
            messageHandler4.SetConsumer(consumer4);
            consumer4.Subscribe("SampleTopic").Start();

            var scheduleService = ObjectContainer.Resolve<IScheduleService>();
            scheduleService.ScheduleTask(() =>
            {
                Console.WriteLine(string.Format("Queue Allocation. c1:{0}, c2:{1}, c3:{2}, c4:{3}",
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
        private Consumer _consumer;

        public void SetConsumer(Consumer consumer)
        {
            _consumer = consumer;
        }
        public void Handle(QueueMessage message)
        {
            Console.WriteLine("[{0}] handled {1}, topic:{2}, queueId:{3}, currentQueues:{4}", _consumer.Id, Encoding.UTF8.GetString(message.Body), message.Topic, message.QueueId, string.Join("|", _consumer.GetCurrentQueues()));
        }
    }
}
