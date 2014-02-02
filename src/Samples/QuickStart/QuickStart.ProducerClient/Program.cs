using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ECommon.Autofac;
using ECommon.Configurations;
using ECommon.IoC;
using ECommon.JsonNet;
using ECommon.Log4Net;
using ECommon.Scheduling;
using EQueue.Clients.Producers;
using EQueue.Configurations;
using EQueue.Protocols;

namespace QuickStart.ProducerClient
{
    class Program
    {
        static void Main(string[] args)
        {
            InitializeEQueue();

            var scheduleService = ObjectContainer.Resolve<IScheduleService>();
            var producer = new Producer().Start();
            var total = 1000;
            var parallelCount = 10;
            var finished = 0;
            var messageIndex = 0;
            var watch = Stopwatch.StartNew();

            var action = new Action(() =>
            {
                for (var index = 1; index <= total; index++)
                {
                    var message = "message" + Interlocked.Increment(ref messageIndex);
                    producer.SendAsync(new Message("SampleTopic", Encoding.UTF8.GetBytes(message)), index.ToString()).ContinueWith(sendTask =>
                    {
                        var finishedCount = Interlocked.Increment(ref finished);
                        if (finishedCount % 1000 == 0)
                        {
                            Console.WriteLine(string.Format("Sent {0} messages, time spent:{1}", finishedCount, watch.ElapsedMilliseconds));
                        }
                    });
                }
            });

            var actions = new List<Action>();
            for (var index = 0; index < parallelCount; index++)
            {
                actions.Add(action);
            }

            Parallel.Invoke(actions.ToArray());

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
}
