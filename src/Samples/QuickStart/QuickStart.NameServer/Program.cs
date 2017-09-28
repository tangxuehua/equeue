using System;
using ECommon.Configurations;
using EQueue.Configurations;
using EQueue.NameServer;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace QuickStart.NameServer
{
    class Program
    {
        static void Main(string[] args)
        {
            InitializeEQueue();
            new NameServerController().Start();
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
                .RegisterEQueueComponents()
                .BuildContainer();
        }
    }
}
