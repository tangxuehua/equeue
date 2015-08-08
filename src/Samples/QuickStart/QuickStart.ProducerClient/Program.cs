using System;
using System.Configuration;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using ECommon.Autofac;
using ECommon.Components;
using ECommon.JsonNet;
using ECommon.Log4Net;
using ECommon.Logging;
using ECommon.Utilities;
using EQueue.Clients.Producers;
using EQueue.Configurations;
using EQueue.Protocols;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace QuickStart.ProducerClient
{
    class Program
    {
        static ILogger _logger;

        static void Main(string[] args)
        {
            InitializeEQueue();
            SendMessage();
            Console.ReadLine();
        }

        static void InitializeEQueue()
        {
            ECommonConfiguration
                .Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4Net()
                .UseJsonNet()
                .RegisterEQueueComponents()
                .SetDefault<IQueueSelector, QueueAverageSelector>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create("Program");
        }
        static void SendMessage()
        {
            var brokerIP = ConfigurationManager.AppSettings["BrokerAddress"];
            var brokerEndPoint = string.IsNullOrEmpty(brokerIP) ? new IPEndPoint(SocketUtils.GetLocalIPV4(), 5000) : new IPEndPoint(IPAddress.Parse(brokerIP), 5000);
            var messageSize = int.Parse(ConfigurationManager.AppSettings["MessageSize"]);
            var connectionCount = int.Parse(ConfigurationManager.AppSettings["ConnectionCount"]);
            var flowControlCount = int.Parse(ConfigurationManager.AppSettings["FlowControlCount"]);
            var payload = new byte[messageSize];
            var message = new Message("SampleTopic", 100, payload);
            var messageIndex = 0L;
            var sendingCount = 0L;
            var finishedCount = 0L;
            var previousFinishedCount = 0L;
            var previousElapsedMilliseconds = 0L;
            var watch = Stopwatch.StartNew();

            for (var i = 0; i < connectionCount; i++)
            {
                Task.Factory.StartNew(() =>
                {
                    var producer = new Producer("Producer@" + ObjectId.GenerateNewStringId(), new ProducerSetting { BrokerProducerIPEndPoint = brokerEndPoint }).Start();

                    while (true)
                    {
                        var waitingCount = sendingCount - finishedCount;
                        if (waitingCount > flowControlCount)
                        {
                            var waitMilliseconds = (waitingCount / flowControlCount) * 1000;
                            _logger.InfoFormat("Start to flow control, pending messages: {1}, wait time: {2}ms", producer.Id, waitingCount, waitMilliseconds);
                            Thread.Sleep((int)waitMilliseconds);
                            continue;
                        }
                        producer.SendAsync(message, Interlocked.Increment(ref messageIndex)).ContinueWith(sendTask =>
                        {
                            if (sendTask.Exception != null)
                            {
                                _logger.ErrorFormat("Sent message failed, errorMessage: {0}", sendTask.Exception.GetBaseException().Message);
                                return;
                            }
                            if (sendTask.Result.SendStatus == SendStatus.Success)
                            {
                                var currentFinishedCount = Interlocked.Increment(ref finishedCount);
                                if (currentFinishedCount % 10000 == 0)
                                {
                                    var currentElapsedMilliseconds = watch.ElapsedMilliseconds;
                                    _logger.InfoFormat("Sent {0} messages, time elapsed: {1}ms, throughput: {2}",
                                        currentFinishedCount,
                                        currentElapsedMilliseconds,
                                        (currentFinishedCount - previousFinishedCount) * 1000 / (currentElapsedMilliseconds - previousElapsedMilliseconds));
                                    Interlocked.Exchange(ref previousFinishedCount, currentFinishedCount);
                                    Interlocked.Exchange(ref previousElapsedMilliseconds, currentElapsedMilliseconds);
                                }
                            }
                            else
                            {
                                _logger.ErrorFormat("Sent message failed, errorMessage: {0}", sendTask.Result.ErrorMessage);
                            }
                        });
                        Interlocked.Increment(ref sendingCount);
                    }
                });
            }
        }
    }
}
