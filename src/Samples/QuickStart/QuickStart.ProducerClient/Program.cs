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
        static long _sendingCount = 0;
        static long _sentCount = 0;
        static ILogger _logger;
        static Stopwatch _watch = new Stopwatch();

        static void Main(string[] args)
        {
            InitializeEQueue();
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
        }
        static void SendMessageTest()
        {
            var serverAddress = ConfigurationManager.AppSettings["ServerAddress"];
            var mode = ConfigurationManager.AppSettings["Mode"];
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
                actions.Add(() => SendMessages(producer, mode, messageCount, sleepMilliseconds, batchSize, message));
            }

            _watch.Start();
            Task.Factory.StartNew(() => Parallel.Invoke(actions.ToArray()));
        }
        static void SendMessages(Producer producer, string mode, int count, int sleepMilliseconds, int batchSize, Message message)
        {
            _logger.InfoFormat("----Send message starting, producerId:{0}----", producer.Id);

            if (mode == "Oneway")
            {
                for (var i = 1; i <= count; i++)
                {
                    TryAction(() => producer.SendOneway(message, message.Key));
                    var current = Interlocked.Increment(ref _sendingCount);
                    if (current % 10000 == 0)
                    {
                        _logger.InfoFormat("Sening {0} messages, timeSpent: {1}ms, throughput: {2}/s", current, _watch.ElapsedMilliseconds, current * 1000 / _watch.ElapsedMilliseconds);
                    }
                    WaitIfNecessory(current, batchSize, sleepMilliseconds);
                }
            }
            else if (mode == "Async")
            {
                for (var i = 1; i <= count; i++)
                {
                    TryAction(() => producer.SendAsync(message, message.Key, 100000).ContinueWith(SendCallback));
                    var current = Interlocked.Increment(ref _sendingCount);
                    WaitIfNecessory(current, batchSize, sleepMilliseconds);
                }
            }
            else if (mode == "Callback")
            {
                producer.RegisterResponseHandler(new ResponseHandler());
                for (var i = 1; i <= count; i++)
                {
                    TryAction(() => producer.SendWithCallback(message, message.Key));
                    var current = Interlocked.Increment(ref _sendingCount);
                    WaitIfNecessory(current, batchSize, sleepMilliseconds);
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

            var current = Interlocked.Increment(ref _sentCount);
            if (current % 10000 == 0)
            {
                _logger.InfoFormat("Sent {0} messages, timeSpent: {1}ms, throughput: {2}/s", current, _watch.ElapsedMilliseconds, current * 1000 / _watch.ElapsedMilliseconds);
            }
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

                var current = Interlocked.Increment(ref _sentCount);
                if (current % 10000 == 0)
                {
                    _logger.InfoFormat("Sent {0} messages, timeSpent: {1}ms, throughput: {2}/s", current, _watch.ElapsedMilliseconds, current * 1000 / _watch.ElapsedMilliseconds);
                }
            }
        }
    }
}
