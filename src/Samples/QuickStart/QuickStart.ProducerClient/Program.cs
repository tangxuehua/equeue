using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
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
            var stopwatch = Stopwatch.StartNew();
            var total = 20000;
            var count = 0;

            for (var index = 1; index <= total; index++)
            {
                var topic = index % 2 == 0 ? "topic1" : "topic2";
                producer.SendAsync(new Message(topic, Encoding.UTF8.GetBytes("Message" + index)), index.ToString()).ContinueWith(sendTask =>
                {
                    var current = Interlocked.Increment(ref count);
                    if (current % 1000 == 0)
                    {
                        Console.WriteLine(sendTask.Result);
                    }
                    if (current == total)
                    {
                        producer.Shutdown();
                        Console.WriteLine("Send message finised, time spent:" + stopwatch.ElapsedMilliseconds);
                    }
                });
            }


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
