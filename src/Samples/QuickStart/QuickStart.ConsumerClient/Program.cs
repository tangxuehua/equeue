using System;
using System.Collections.Generic;
using System.Configuration;
using System.Net;
using System.Threading;
using ECommon.Components;
using ECommon.Configurations;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Socketing;
using EQueue.Clients.Consumers;
using EQueue.Configurations;
using EQueue.Protocols;
using EQueue.Utils;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace QuickStart.ConsumerClient
{
    class Program
    {
        static void Main(string[] args)
        {
            InitializeEQueue();
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
                .RegisterEQueueComponents();

            var clusterName = ConfigurationManager.AppSettings["ClusterName"];
            var consumerName = ConfigurationManager.AppSettings["ConsumerName"];
            var consumerGroup = ConfigurationManager.AppSettings["ConsumerGroup"];
            var address = ConfigurationManager.AppSettings["NameServerAddress"];
            var topic = ConfigurationManager.AppSettings["Topic"];
            var nameServerAddress = string.IsNullOrEmpty(address) ? SocketUtils.GetLocalIPV4() : IPAddress.Parse(address);
            var clientCount = int.Parse(ConfigurationManager.AppSettings["ClientCount"]);
            var setting = new ConsumerSetting
            {
                ClusterName = clusterName,
                ConsumeFromWhere = ConsumeFromWhere.FirstOffset,
                MessageHandleMode = MessageHandleMode.Sequential,
                NameServerList = new List<IPEndPoint> { new IPEndPoint(nameServerAddress, 9493) }
            };
            var messageHandler = new MessageHandler();
            for (var i = 1; i <= clientCount; i++)
            {
                new Consumer(consumerGroup, setting, consumerName)
                    .Subscribe(topic)
                    .SetMessageHandler(messageHandler)
                    .Start();
            }
        }

        class MessageHandler : IMessageHandler
        {
            private long _previusHandledCount;
            private long _handledCount;
            private long _calculateCount = 0;
            private IScheduleService _scheduleService;
            private IRTStatisticService _rtStatisticService;
            private ILogger _logger;

            public MessageHandler()
            {
                _scheduleService = ObjectContainer.Resolve<IScheduleService>();
                _scheduleService.StartTask("PrintThroughput", PrintThroughput, 1000, 1000);
                _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(typeof(Program).Name);
                _rtStatisticService = ObjectContainer.Resolve<IRTStatisticService>();
            }

            public void Handle(QueueMessage message, IMessageContext context)
            {
                Interlocked.Increment(ref _handledCount);
                _rtStatisticService.AddRT((DateTime.Now - message.CreatedTime).TotalMilliseconds);
                context.OnMessageHandled(message);
            }

            private void PrintThroughput()
            {
                var totalHandledCount = _handledCount;
                var throughput = totalHandledCount - _previusHandledCount;
                _previusHandledCount = totalHandledCount;
                if (throughput > 0)
                {
                    _calculateCount++;
                }

                var average = 0L;
                if (_calculateCount > 0)
                {
                    average = totalHandledCount / _calculateCount;
                }

                _logger.InfoFormat("totalReceived: {0}, throughput: {1}/s, average: {2}, delay: {3:F3}ms", totalHandledCount, throughput, average, _rtStatisticService.ResetAndGetRTStatisticInfo());
            }
        }
    }
}
