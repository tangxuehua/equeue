using System;
using System.Text;
using EQueue;
using EQueue.Autofac;
using EQueue.Clients.Consumers;
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

            var consumer1 = new Consumer("consumer1", ConsumerSettings.Default, "group1", MessageModel.Clustering, new MessageHandler("consumer1"))
                .Subscribe("SampleTopic")
                .Start();
            var consumer2 = new Consumer("consumer2", ConsumerSettings.Default, "group1", MessageModel.Clustering, new MessageHandler("consumer2"))
                .Subscribe("SampleTopic")
                .Start();
            var consumer3 = new Consumer("consumer3", ConsumerSettings.Default, "group1", MessageModel.Clustering, new MessageHandler("consumer3"))
                .Subscribe("SampleTopic")
                .Start();
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
        private string _consumerId;

        public MessageHandler(string consumerId)
        {
            _consumerId = consumerId;
        }
        public void Handle(QueueMessage message)
        {
            Console.WriteLine("[{0}] handled {1}, topic:{2}, queueId:{3}", _consumerId, Encoding.UTF8.GetString(message.Body), message.Topic, message.QueueId);
        }
    }
}
