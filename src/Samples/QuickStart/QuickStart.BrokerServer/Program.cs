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
            setting.DeleteMessageInterval = 1000;
            new BrokerController(setting).Initialize().Start();
            Console.ReadLine();
        }

        static void InitializeEQueue()
        {
            var messageStoreSetting = new SqlServerMessageStoreSetting
            {
                ConnectionString = "Data Source=(local);Initial Catalog=equeue;Integrated Security=True;",
                DeleteMessageHourOfDay = -1
            };
            Configuration
                .Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4Net()
                .UseJsonNet()
                .RegisterEQueueComponents()
                .UseSqlServerMessageStore(messageStoreSetting);
        }
    }
}
