using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ECommon.Autofac;
using ECommon.Configurations;
using ECommon.Components;
using ECommon.JsonNet;
using ECommon.Log4Net;
using ECommon.Logging;
using ECommon.Scheduling;
using EQueue.Clients.Producers;
using EQueue.Configurations;
using EQueue.Protocols;

namespace QuickStart.ProducerClient
{
    class IndexEntry
    {
        public int Index;
        public int Total;
    }
    class Program
    {
        static int finished;
        static int messageIndex;
        static Stopwatch watch = Stopwatch.StartNew();

        static void SendMessage(Producer producer, IndexEntry indexEntry)
        {
            var message = "message" + Interlocked.Increment(ref messageIndex);
            producer.SendAsync(new Message("SampleTopic", Encoding.UTF8.GetBytes(message)), indexEntry.Index.ToString()).ContinueWith(sendTask =>
            {
                var finishedCount = Interlocked.Increment(ref finished);
                if (finishedCount % 1000 == 0)
                {
                    _logger.InfoFormat("Sent {0} messages, time spent:{1}", finishedCount, watch.ElapsedMilliseconds);
                }
                if (indexEntry.Index < indexEntry.Total)
                {
                    indexEntry.Index++;
                    SendMessage(producer, indexEntry);
                }
            });
        }
        static void Main(string[] args)
        {
            InitializeEQueue();

            var scheduleService = ObjectContainer.Resolve<IScheduleService>();
            var producer = new Producer("Producer1").Start();
            var total = 10000;
            var parallelCount = 10;

            var action = new Action(() =>
            {
                SendMessage(producer, new IndexEntry { Index = 1, Total = total });
            });

            var actions = new List<Action>();
            for (var index = 0; index < parallelCount; index++)
            {
                actions.Add(action);
            }

            Parallel.Invoke(actions.ToArray());

            Console.ReadLine();
        }

        static ILogger _logger;
        static void InitializeEQueue()
        {
            Configuration
                .Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4Net()
                .UseJsonNet()
                .RegisterEQueueComponents()
                .SetDefault<IQueueSelector, QueueAverageSelector>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create("Program");
        }
    }
}
