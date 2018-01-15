using System;
using System.Collections.Generic;
using System.Configuration;
using System.Net;
using ECommon.Components;
using ECommon.Configurations;
using ECommon.Socketing;
using ECommon.Utilities;
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
                .RegisterEQueueComponents()
                .BuildContainer();

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
            private readonly IPerformanceService _performanceService;

            public MessageHandler()
            {
                _performanceService = ObjectContainer.Resolve<IPerformanceService>();
                _performanceService.Initialize("TotalReceived").Start();
            }

            public void Handle(QueueMessage message, IMessageContext context)
            {
                _performanceService.IncrementKeyCount("default", (DateTime.Now - message.CreatedTime).TotalMilliseconds);
                context.OnMessageHandled(message);
            }
        }
    }
}
