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

            var producer1 = new Producer().Start();
            var producer2 = new Producer().Start();
            var producer3 = new Producer().Start();
            var stopwatch1 = Stopwatch.StartNew();
            var stopwatch2 = Stopwatch.StartNew();
            var stopwatch3 = Stopwatch.StartNew();
            var total = 20000;
            var count1 = 0;
            var count2 = 0;
            var count3 = 0;

            Task.Factory.StartNew(() =>
            {
                for (var index = 1; index <= total; index++)
                {
                    var topic = index % 2 == 0 ? "topic1" : "topic1";
                    producer1.SendAsync(new Message(topic, Encoding.UTF8.GetBytes("Message" + index)), index.ToString()).ContinueWith(sendTask =>
                    {
                        var current = Interlocked.Increment(ref count1);
                        if (current % 1000 == 0)
                        {
                            Console.WriteLine("Producer1:" + sendTask.Result);
                        }
                        if (current == total)
                        {
                            //producer1.Shutdown();
                            Console.WriteLine("Producer1 send message finised, time spent:" + stopwatch1.ElapsedMilliseconds + ", messageOffset:" + sendTask.Result.MessageOffset);
                        }
                    });
                }
            });
            Task.Factory.StartNew(() =>
            {
                for (var index = 1; index <= total; index++)
                {
                    var topic = index % 2 == 0 ? "topic1" : "topic1";
                    producer2.SendAsync(new Message(topic, Encoding.UTF8.GetBytes("Message" + index)), index.ToString()).ContinueWith(sendTask =>
                    {
                        var current = Interlocked.Increment(ref count2);
                        if (current % 1000 == 0)
                        {
                            Console.WriteLine("Producer2:" + sendTask.Result);
                        }
                        if (current == total)
                        {
                            //producer2.Shutdown();
                            Console.WriteLine("Producer2 send message finised, time spent:" + stopwatch2.ElapsedMilliseconds + ", messageOffset:" + sendTask.Result.MessageOffset);
                        }
                    });
                }
            });
            Task.Factory.StartNew(() =>
            {
                for (var index = 1; index <= total; index++)
                {
                    var topic = index % 2 == 0 ? "topic1" : "topic1";
                    producer3.SendAsync(new Message(topic, Encoding.UTF8.GetBytes("Message" + index)), index.ToString()).ContinueWith(sendTask =>
                    {
                        var current = Interlocked.Increment(ref count3);
                        if (current % 1000 == 0)
                        {
                            Console.WriteLine("Producer3:" + sendTask.Result);
                        }
                        if (current == total)
                        {
                            //producer3.Shutdown();
                            Console.WriteLine("Producer3 send message finised, time spent:" + stopwatch3.ElapsedMilliseconds + ", messageOffset:" + sendTask.Result.MessageOffset);
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
