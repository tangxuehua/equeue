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
            setting.NotifyWhenMessageArrived = true;
            setting.RemoveConsumedMessageInterval = 1000;
            new BrokerController(setting).Start();
            Console.ReadLine();
        }

        static void InitializeEQueue()
        {
            var connectionString = @"Server=(local);Initial Catalog=EQueue;uid=sa;pwd=howareyou;Connect Timeout=30;Min Pool Size=10;Max Pool Size=100";
            var messageStoreSetting = new SqlServerMessageStoreSetting
            {
                ConnectionString = connectionString,
                DeleteMessageHourOfDay = -1
            };
            var offsetManagerSetting = new SqlServerOffsetManagerSetting
            {
                ConnectionString = connectionString
            };

            Configuration
                .Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4Net()
                .UseJsonNet()
                .RegisterEQueueComponents()
                .UseSqlServerMessageStore(messageStoreSetting)
                .UseSqlServerOffsetManager(offsetManagerSetting);
        }
    }
}
