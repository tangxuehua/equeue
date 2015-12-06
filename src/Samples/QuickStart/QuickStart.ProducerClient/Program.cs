using System;
using System.Collections.Generic;
using System.Configuration;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using ECommon.Components;
using ECommon.Configurations;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Scheduling;
using ECommon.Socketing;
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
        static long _calculateCount = 0;
        static string _mode;
        static bool _hasError;
        static ILogger _logger;
        static IScheduleService _scheduleService;

        static void Main(string[] args)
        {
            InitializeEQueue();
            SendMessageTest();
            StartPrintThroughputTask();
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

            var address = ConfigurationManager.AppSettings["BrokerAddress"];
            var brokerAddress = string.IsNullOrEmpty(address) ? SocketUtils.GetLocalIPV4() : IPAddress.Parse(address);
            var clientCount = int.Parse(ConfigurationManager.AppSettings["ClientCount"]);
            var messageSize = int.Parse(ConfigurationManager.AppSettings["MessageSize"]);
            var messageCount = int.Parse(ConfigurationManager.AppSettings["MessageCount"]);
            var actions = new List<Action>();
            var payload = new byte[messageSize];
            var topic = ConfigurationManager.AppSettings["Topic"];
            var message = new Message(topic, 100, payload);

            for (var i = 0; i < clientCount; i++)
            {
                var setting = new ProducerSetting
                {
                    BrokerAddress = new IPEndPoint(brokerAddress, 5000),
                    BrokerAdminAddress = new IPEndPoint(brokerAddress, 5002)
                };
                var producer = new Producer(setting).Start();
                actions.Add(() => SendMessages(producer, _mode, messageCount, message));
            }

            Task.Factory.StartNew(() => Parallel.Invoke(actions.ToArray()));
        }
        static void SendMessages(Producer producer, string mode, int messageCount, Message message)
        {
            _logger.Info("----Send message starting----");

            var sendAction = default(Action<int>);

            if (_mode == "Oneway")
            {
                sendAction = index =>
                {
                    producer.SendOneway(message, index.ToString());
                    Interlocked.Increment(ref _sentCount);
                };
            }
            else if (_mode == "Sync")
            {
                sendAction = index =>
                {
                    var result = producer.Send(message, index.ToString());
                    if (result.SendStatus != SendStatus.Success)
                    {
                        throw new Exception(result.ErrorMessage);
                    }
                    Interlocked.Increment(ref _sentCount);
                };
            }
            else if (_mode == "Async")
            {
                sendAction = index => producer.SendAsync(message, index.ToString()).ContinueWith(t =>
                {
                    if (t.Exception != null)
                    {
                        _hasError = true;
                        _logger.ErrorFormat("Send message has exception, errorMessage: {0}", t.Exception.GetBaseException().Message);
                        return;
                    }
                    if (t.Result == null)
                    {
                        _hasError = true;
                        _logger.Error("Send message timeout.");
                        return;
                    }
                    if (t.Result.SendStatus != SendStatus.Success)
                    {
                        _hasError = true;
                        _logger.ErrorFormat("Send message failed, errorMessage: {0}", t.Result.ErrorMessage);
                        return;
                    }

                    Interlocked.Increment(ref _sentCount);
                });
            }
            else if (_mode == "Callback")
            {
                producer.RegisterResponseHandler(new ResponseHandler());
                sendAction = index => producer.SendWithCallback(message, index.ToString());
            }

            Task.Factory.StartNew(() =>
            {
                for (var i = 0; i < messageCount; i++)
                {
                    try
                    {
                        sendAction(i);
                    }
                    catch (Exception ex)
                    {
                        _hasError = true;
                        _logger.ErrorFormat("Send message failed, errorMsg:{0}", ex.Message);
                    }

                    if (_hasError)
                    {
                        Thread.Sleep(3000);
                        _hasError = false;
                    }
                }
            });
        }

        static void StartPrintThroughputTask()
        {
            _scheduleService.StartTask("PrintThroughput", PrintThroughput, 1000, 1000);
        }
        static void PrintThroughput()
        {
            var totalSentCount = _sentCount;
            var throughput = totalSentCount - _previousSentCount;
            _previousSentCount = totalSentCount;
            if (throughput > 0)
            {
                _calculateCount++;
            }

            var average = 0L;
            if (_calculateCount > 0)
            {
                average = totalSentCount / _calculateCount;
            }
            _logger.InfoFormat("Send message mode: {0}, totalSent: {1}, throughput: {2}/s, average: {3}", _mode, totalSentCount, throughput, average);
        }

        class ResponseHandler : IResponseHandler
        {
            public void HandleResponse(RemotingResponse remotingResponse)
            {
                var sendResult = Producer.ParseSendResult(remotingResponse);
                if (sendResult.SendStatus != SendStatus.Success)
                {
                    _hasError = true;
                    _logger.Error(sendResult.ErrorMessage);
                    return;
                }

                Interlocked.Increment(ref _sentCount);
            }
        }
    }
}
