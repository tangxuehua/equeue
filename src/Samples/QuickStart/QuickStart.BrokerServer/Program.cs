using System;
using System.Configuration;
using ECommon.Autofac;
using ECommon.JsonNet;
using ECommon.Log4Net;
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
            BrokerController.Create(new BrokerSetting { NotifyWhenMessageArrived = false }).Start();
            Console.ReadLine();
        }

        static void InitializeEQueue()
        {
            var configuration = ECommonConfiguration
                .Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4Net()
                .UseJsonNet()
                .RegisterUnhandledExceptionHandler()
                .RegisterEQueueComponents();

            var persistMode = ConfigurationManager.AppSettings["persistMode"];
            var maxCacheMessageSize = int.Parse(ConfigurationManager.AppSettings["maxCacheMessageSize"]);

            if (persistMode == "in-memory")
            {
                configuration.UseInMemoryMessageStore(new InMemoryMessageStoreSetting { MessageMaxCacheSize = maxCacheMessageSize });
            }
            else if (persistMode == "sql")
            {
                var connectionString = ConfigurationManager.AppSettings["connectionString"];
                var persistMessageInterval = int.Parse(ConfigurationManager.AppSettings["persistMessageInterval"]);
                var persistMessageMaxCount = int.Parse(ConfigurationManager.AppSettings["persistMessageMaxCount"]);
                var messageLogFile = ConfigurationManager.AppSettings["messageLogFile"];

                var queueStoreSetting = new SqlServerQueueStoreSetting
                {
                    ConnectionString = connectionString
                };
                var messageStoreSetting = new SqlServerMessageStoreSetting
                {
                    ConnectionString = connectionString,
                    PersistMessageInterval = persistMessageInterval,
                    PersistMessageMaxCount = persistMessageMaxCount,
                    MessageLogFile = messageLogFile,
                    MessageMaxCacheSize = maxCacheMessageSize
                };
                var offsetManagerSetting = new SqlServerOffsetManagerSetting
                {
                    ConnectionString = connectionString
                };

                configuration
                    .UseSqlServerQueueStore(queueStoreSetting)
                    .UseSqlServerMessageStore(messageStoreSetting)
                    .UseSqlServerOffsetManager(offsetManagerSetting);
            }

        }
    }
}
