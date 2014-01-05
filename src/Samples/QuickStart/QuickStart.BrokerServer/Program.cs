using System;
using System.Text;
using EQueue;
using EQueue.Autofac;
using EQueue.JsonNet;
using EQueue.Log4Net;
using EQueue.Protocols;

namespace QuickStart.BrokerServer
{
    class Program
    {
        static void Main(string[] args)
        {
            InitializeEQueue();
        }

        static void InitializeEQueue()
        {
            Configuration
                .Create()
                .UseAutofac()
                .RegisterFrameworkComponents()
                .UseLog4Net()
                .UseJsonNet();
        }
    }
}
