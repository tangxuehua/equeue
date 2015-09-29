using System;
using System.Collections.Generic;
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
using ECommon.Remoting;
using ECommon.Scheduling;
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
        static long _previousSentCount = 0;
        static long _sentCount = 0;
        static string _mode;
        static ILogger _logger;
        static Stopwatch _watch = new Stopwatch();
        static IScheduleService _scheduleService;

        static void Main(string[] args)
        {
            InitializeEQueue();
            StartPrintThroughputTask();
            SendMessageTest();
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
                .RegisterUnhandledExceptionHandler()
                .RegisterEQueueComponents()
                .SetDefault<IQueueSelector, QueueAverageSelector>();

            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(typeof(Program).Name);
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
        }
        static void SendMessageTest()
        {
            _mode = ConfigurationManager.AppSettings["Mode"];

            var serverAddress = ConfigurationManager.AppSettings["ServerAddress"];
            var brokerAddress = string.IsNullOrEmpty(serverAddress) ? SocketUtils.GetLocalIPV4() : IPAddress.Parse(serverAddress);
            var clientCount = int.Parse(ConfigurationManager.AppSettings["ClientCount"]);
            var messageSize = int.Parse(ConfigurationManager.AppSettings["MessageSize"]);
            var messageCount = int.Parse(ConfigurationManager.AppSettings["MessageCount"]);
            var sleepMilliseconds = int.Parse(ConfigurationManager.AppSettings["SleepMilliseconds"]);
            var batchSize = int.Parse(ConfigurationManager.AppSettings["BatchSize"]);
            var actions = new List<Action>();
            var payload = new byte[messageSize];
            var message = new Message("topic1", 100, ObjectId.GenerateNewStringId(), payload);

            for (var i = 1; i <= clientCount; i++)
            {
                var producer = new Producer("Producer@" + i.ToString(), new ProducerSetting { BrokerAddress = new IPEndPoint(brokerAddress, 5000) }).Start();
                actions.Add(() => SendMessages(producer, _mode, messageCount, sleepMilliseconds, batchSize, message));
            }

            _watch.Start();
            Task.Factory.StartNew(() => Parallel.Invoke(actions.ToArray()));
        }
        static void SendMessages(Producer producer, string mode, int count, int sleepMilliseconds, int batchSize, Message message)
        {
            _logger.InfoFormat("----Send message starting, producerId:{0}----", producer.Id);
            var sendingCount = 0L;

            if (mode == "Oneway")
            {
                for (var i = 1; i <= count; i++)
                {
                    TryAction(() => producer.SendOneway(message, message.Key));
                    Interlocked.Increment(ref _sentCount);
                    WaitIfNecessory(Interlocked.Increment(ref sendingCount), batchSize, sleepMilliseconds);
                }
            }
            else if (mode == "Async")
            {
                for (var i = 1; i <= count; i++)
                {
                    TryAction(() => producer.SendAsync(message, message.Key, 100000).ContinueWith(SendCallback));
                    WaitIfNecessory(Interlocked.Increment(ref sendingCount), batchSize, sleepMilliseconds);
                }
            }
            else if (mode == "Callback")
            {
                producer.RegisterResponseHandler(new ResponseHandler());
                for (var i = 1; i <= count; i++)
                {
                    TryAction(() => producer.SendWithCallback(message, message.Key));
                    WaitIfNecessory(Interlocked.Increment(ref sendingCount), batchSize, sleepMilliseconds);
                }
            }
        }
        static void SendCallback(Task<SendResult> task)
        {
            if (task.Exception != null)
            {
                _logger.ErrorFormat("Send message has exception, errorMessage: {0}", task.Exception.GetBaseException().Message);
                return;
            }
            if (task.Result == null)
            {
                _logger.Error("Send message timeout.");
                return;
            }
            if (task.Result.SendStatus != SendStatus.Success)
            {
                _logger.ErrorFormat("Send message failed, errorMessage: {0}", task.Result.ErrorMessage);
            }

            Interlocked.Increment(ref _sentCount);
        }
        static void TryAction(Action sendMessageAction)
        {
            try
            {
                sendMessageAction();
            }
            catch (Exception ex)
            {
                _logger.ErrorFormat("Send message failed, errorMsg:{0}", ex.Message);
                Thread.Sleep(5000);
            }
        }
        static void WaitIfNecessory(long current, int batchSize, int sleepMilliseconds)
        {
            if (current % batchSize == 0)
            {
                Thread.Sleep(sleepMilliseconds);
            }
        }
        static void StartPrintThroughputTask()
        {
            _scheduleService.StartTask("Program.PrintThroughput", PrintThroughput, 0, 1000);
        }
        static void PrintThroughput()
        {
            var totalSentCount = _sentCount;
            var totalCountOfCurrentPeriod = totalSentCount - _previousSentCount;
            _previousSentCount = totalSentCount;

            _logger.InfoFormat("Send message mode: {0}, currentTime: {1}, totalSent: {2}, throughput: {3}/s", _mode, DateTime.Now.ToLongTimeString(), totalSentCount, totalCountOfCurrentPeriod);
        }

        class ResponseHandler : IResponseHandler
        {
            public void HandleResponse(RemotingResponse remotingResponse)
            {
                var sendResult = Producer.ParseSendResult(remotingResponse);
                if (sendResult.SendStatus != SendStatus.Success)
                {
                    _logger.Error(sendResult.ErrorMessage);
                    return;
                }

                Interlocked.Increment(ref _sentCount);
            }
        }
    }
}
