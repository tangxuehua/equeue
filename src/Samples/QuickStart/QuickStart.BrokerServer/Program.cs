using System;
using System.Configuration;
using System.Net;
using ECommon.Autofac;
using ECommon.JsonNet;
using ECommon.Log4Net;
using ECommon.Socketing;
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
                ProducerAddress = new IPEndPoint(brokerEndPoint, 5000),
                ConsumerAddress = new IPEndPoint(brokerEndPoint, 5001),
                AdminAddress = new IPEndPoint(brokerEndPoint, 5002),
                NotifyWhenMessageArrived = bool.Parse(ConfigurationManager.AppSettings["NotifyWhenMessageArrived"])
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
