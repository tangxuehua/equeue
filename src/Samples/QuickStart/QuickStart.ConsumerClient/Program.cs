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

            var consumer = new Consumer(ConsumerSettings.Default, "group1", MessageModel.Clustering, new MessageHandler())
                .Subscribe("topic1")
                .Subscribe("topic2")
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
        public void Handle(Message message)
        {
            Console.WriteLine("Handled {0}", Encoding.UTF8.GetString(message.Body));
        }
    }
}
