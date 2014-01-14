using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
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
            var stopwatch = Stopwatch.StartNew();
            var total = 2;
            var count = 0;

            Task.Factory.StartNew(() =>
            {
                for (var index = 1; index <= total; index++)
                {
                    producer.SendAsync(new Message("topic1", Encoding.UTF8.GetBytes("topic1-message" + index)), index.ToString()).ContinueWith(sendTask =>
                    {
                        var current = Interlocked.Increment(ref count);
                        if (current == total)
                        {
                            //producer.Shutdown();
                            Console.WriteLine("Send message finised, time spent:" + stopwatch.ElapsedMilliseconds + ", messageOffset:" + sendTask.Result.MessageOffset);
                        }
                    });
                }
            });
            Task.Factory.StartNew(() =>
            {
                for (var index = 1; index <= total; index++)
                {
                    producer.SendAsync(new Message("topic2", Encoding.UTF8.GetBytes("topic2-message" + index)), index.ToString()).ContinueWith(sendTask =>
                    {
                        var current = Interlocked.Increment(ref count);
                        if (current == total)
                        {
                            //producer.Shutdown();
                            Console.WriteLine("Send message finised, time spent:" + stopwatch.ElapsedMilliseconds + ", messageOffset:" + sendTask.Result.MessageOffset);
                        }
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
