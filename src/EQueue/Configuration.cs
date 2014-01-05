using System;
using EQueue.Broker;
using EQueue.Clients.Consumers;
using EQueue.Infrastructure.IoC;
using EQueue.Infrastructure.Logging;
using EQueue.Infrastructure.Scheduling;

namespace EQueue
{
    public class Configuration
    {
        public static Configuration Instance { get; private set; }

        private Configuration() { }

        public static Configuration Create()
        {
            if (Instance != null)
            {
                throw new Exception("Could not create configuration instance twice.");
            }
            Instance = new Configuration();
            return Instance;
        }

        public Configuration SetDefault<TService, TImplementer>(LifeStyle life = LifeStyle.Singleton)
            where TService : class
            where TImplementer : class, TService
        {
            ObjectContainer.Register<TService, TImplementer>(life);
            return this;
        }
        public Configuration SetDefault<TService, TImplementer>(TImplementer instance)
            where TService : class
            where TImplementer : class, TService
        {
            ObjectContainer.RegisterInstance<TService, TImplementer>(instance);
            return this;
        }

        public Configuration RegisterFrameworkComponents()
        {
            ObjectContainer.Register<IScheduleService, ScheduleService>();
            ObjectContainer.Register<IAllocateMessageQueueStrategy, AverageAllocateMessageQueueStrategy>();
            ObjectContainer.Register<IMessageService, MessageService>();
            return this;
        }
    }
}
