using System;
using System.Text;
using EQueue;
using EQueue.Autofac;
using EQueue.Clients.Producers;
using EQueue.JsonNet;
using EQueue.Log4Net;
using EQueue.Protocols;

namespace QuickStart.ProducerClient
{
    class Program
    {
        static void Main(string[] args)
        {
            InitializeEQueue();

            var producer = new Producer().Start();

            for (var index = 1; index <= 20; index++)
            {
                var topic = index % 2 == 0 ? "topic1" : "topic2";
                producer.SendAsync(new Message(topic, Encoding.UTF8.GetBytes("Message" + index)), index.ToString()).ContinueWith(task =>
                {
                    Console.WriteLine(task.Result);
                });
            }

            Console.WriteLine("Producer started...");
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
}
