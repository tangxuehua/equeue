using System;
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

            var address = ConfigurationManager.AppSettings["BrokerAddress"];
            var brokerAddress = string.IsNullOrEmpty(address) ? SocketUtils.GetLocalIPV4() : IPAddress.Parse(address);
            var clientCount = int.Parse(ConfigurationManager.AppSettings["ClientCount"]);
            var setting = new ConsumerSetting
            {
                ConsumeFromWhere = ConsumeFromWhere.FirstOffset,
                MessageHandleMode = MessageHandleMode.Parallel,
                BrokerAddress = new IPEndPoint(brokerAddress, 5001),
                BrokerAdminAddress = new IPEndPoint(brokerAddress, 5002)
            };
            var messageHandler = new MessageHandler();
            for (var i = 1; i <= clientCount; i++)
            {
                new Consumer(ConfigurationManager.AppSettings["ConsumerGroup"], setting)
                    .Subscribe(ConfigurationManager.AppSettings["Topic"])
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
            private ILogger _logger;

            public MessageHandler()
            {
                _scheduleService = ObjectContainer.Resolve<IScheduleService>();
                _scheduleService.StartTask("PrintThroughput", PrintThroughput, 1000, 1000);
                _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(typeof(Program).Name);
            }

            public void Handle(QueueMessage message, IMessageContext context)
            {
                Interlocked.Increment(ref _handledCount);
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

                _logger.InfoFormat("totalReceived: {0}, throughput: {1}/s, average: {2}", totalHandledCount, throughput, average);
            }
        }
    }
}
