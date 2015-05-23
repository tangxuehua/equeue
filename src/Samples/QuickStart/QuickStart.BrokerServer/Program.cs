using System;
using System.Configuration;
using System.Net;
using ECommon.Autofac;
using ECommon.Configurations;
using ECommon.JsonNet;
using ECommon.Log4Net;
using ECommon.Utilities;
using EQueue.Broker;
using EQueue.Configurations;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace QuickStart.BrokerServer
{
    class Program
    {
        static void Main(string[] args)
        {
            InitializeEQueue();

            var bindingIP = ConfigurationManager.AppSettings["BindingAddress"];
            var brokerEndPoint = string.IsNullOrEmpty(bindingIP) ? SocketUtils.GetLocalIPV4() : IPAddress.Parse(bindingIP);
            var setting = new BrokerSetting {
                ProducerIPEndPoint = new IPEndPoint(brokerEndPoint, 5000),
                ConsumerIPEndPoint = new IPEndPoint(brokerEndPoint, 5001),
                AdminIPEndPoint = new IPEndPoint(brokerEndPoint, 5002)
            };
            BrokerController.Create(setting).Start();
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
                .RegisterEQueueComponents();
        }
    }
}
