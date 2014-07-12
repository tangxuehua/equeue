using System;
using ECommon.Autofac;
using ECommon.Configurations;
using ECommon.JsonNet;
using ECommon.Log4Net;
using EQueue.Broker;
using EQueue.Configurations;

namespace QuickStart.BrokerServer
{
    class Program
    {
        static void Main(string[] args)
        {
            InitializeEQueue();
            var setting = new BrokerSetting();
            setting.NotifyWhenMessageArrived = false;
            setting.RemoveMessageInterval = 1000;
            new BrokerController(setting).Start();
            Console.ReadLine();
        }

        static void InitializeEQueue()
        {
            Configuration
                .Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4Net()
                .UseJsonNet()
                .RegisterEQueueComponents();
        }
    }
}
