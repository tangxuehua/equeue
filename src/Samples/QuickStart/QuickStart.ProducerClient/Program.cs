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
using ECommon.Socketing;
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
        static ILogger _fileLogger;

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
            _fileLogger = ObjectContainer.Resolve<ILoggerFactory>().Create("FileLogger");
        }
        static void SendMessage()
        {
            var brokerIP = ConfigurationManager.AppSettings["BrokerAddress"];
            var brokerEndPoint = string.IsNullOrEmpty(brokerIP) ? new IPEndPoint(SocketUtils.GetLocalIPV4(), 5000) : new IPEndPoint(IPAddress.Parse(brokerIP), 5000);
            var messageSize = int.Parse(ConfigurationManager.AppSettings["MessageSize"]);
            var connectionCount = int.Parse(ConfigurationManager.AppSettings["ConnectionCount"]);
            var flowControlCount = int.Parse(ConfigurationManager.AppSettings["FlowControlCount"]);
            var maxMessageCount = int.Parse(ConfigurationManager.AppSettings["MaxMessageCount"]);
            var payload = new byte[messageSize];
            var message = new Message("SampleTopic", 100, ObjectId.GenerateNewStringId(), payload);
            var messageIndex = 0L;
            var sendingCount = 0L;
            var finishedCount = 0L;
            var previousFinishedCount = 0L;
            var previousElapsedMilliseconds = 0L;
            var flowControlledCount = 0;
            var watch = Stopwatch.StartNew();
            var sendOneway = bool.Parse(ConfigurationManager.AppSettings["SendOneway"]);

            for (var i = 0; i < connectionCount; i++)
            {
                Task.Factory.StartNew(() =>
                {
                    var producer = new Producer("Producer@" + ObjectId.GenerateNewStringId(), new ProducerSetting { BrokerAddress = brokerEndPoint }).Start();

                    while (true)
                    {
                        if (sendOneway)
                        {
                            producer.SendOneway(message, Interlocked.Increment(ref messageIndex).ToString());
                            var currentCount = Interlocked.Increment(ref finishedCount);
                            if (currentCount % 10000 == 0)
                            {
                                var currentElapsedMilliseconds = watch.ElapsedMilliseconds;
                                _logger.InfoFormat("Sent {0} messages, time elapsed: {1}ms, average throughput: {2}",
                                    currentCount,
                                    currentElapsedMilliseconds,
                                    currentCount * 1000 / currentElapsedMilliseconds);
                            }
                        }
                        else
                        {
                            var waitingCount = sendingCount - finishedCount;
                            if (waitingCount > flowControlCount)
                            {
                                Thread.Sleep(1);
                                var current = Interlocked.Increment(ref flowControlledCount);
                                if (current % 1000 == 0)
                                {
                                    _logger.InfoFormat("Start to flow control, pending messages: {0}, flow count: {1}", waitingCount, current);
                                }
                                continue;
                            }

                            producer.SendAsync(message, Interlocked.Increment(ref messageIndex).ToString(), 300000).ContinueWith(sendTask =>
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
                                        _fileLogger.InfoFormat("{0},{1}",
                                            (currentFinishedCount - previousFinishedCount) * 1000 / (currentElapsedMilliseconds - previousElapsedMilliseconds),
                                            (currentElapsedMilliseconds - previousElapsedMilliseconds) * 100 / (currentFinishedCount - previousFinishedCount));
                                        _logger.InfoFormat("Sent {0} messages, time elapsed: {1}ms, current throughput: {2}, average throughput: {3}",
                                            currentFinishedCount,
                                            currentElapsedMilliseconds,
                                            (currentFinishedCount - previousFinishedCount) * 1000 / (currentElapsedMilliseconds - previousElapsedMilliseconds),
                                            currentFinishedCount * 1000 / currentElapsedMilliseconds);
                                        Interlocked.Exchange(ref previousFinishedCount, currentFinishedCount);
                                        Interlocked.Exchange(ref previousElapsedMilliseconds, currentElapsedMilliseconds);
                                    }
                                }
                                else
                                {
                                    _logger.ErrorFormat("Sent message failed, errorMessage: {0}", sendTask.Result.ErrorMessage);
                                }
                            });
                        }
                        var count = Interlocked.Increment(ref sendingCount);
                        if (count >= maxMessageCount)
                        {
                            break;
                        }
                    }
                });
            }
        }
    }
}
