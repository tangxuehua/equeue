using System;
using System.Text;
using System.Threading.Tasks;
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
            var total = 8;

            Task.Factory.StartNew(() =>
            {
                for (var index = 1; index <= total; index++)
                {
                    var message = "topic1-message" + index;
                    producer.SendAsync(new Message("topic1", Encoding.UTF8.GetBytes(message)), index.ToString()).ContinueWith(sendTask =>
                    {
                        Console.WriteLine("Sent:" + message);
                    });
                }
            });
            Task.Factory.StartNew(() =>
            {
                for (var index = 1; index <= total; index++)
                {
                    var message = "topic2-message" + index;
                    producer.SendAsync(new Message("topic2", Encoding.UTF8.GetBytes(message)), index.ToString()).ContinueWith(sendTask =>
                    {
                        Console.WriteLine("Sent:" + message);
                    });
                }
            });

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
