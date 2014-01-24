using ECommon.Configurations;
using ECommon.IoC;
using ECommon.Scheduling;
using ECommon.Serializing;
using EQueue.Broker;
using EQueue.Clients.Consumers;
using EQueue.Clients.Producers;

namespace EQueue.Configurations
{
    public static class ConfigurationExtensions
    {
        public static Configuration RegisterEQueueComponents(this Configuration configuration)
        {
            ObjectContainer.Register<IBinarySerializer, JsonBasedBinarySerializer>();
            ObjectContainer.Register<IScheduleService, ScheduleService>();
            ObjectContainer.Register<IAllocateMessageQueueStrategy, AverageAllocateMessageQueueStrategy>();
            ObjectContainer.Register<IQueueSelector, QueueHashSelector>();
            ObjectContainer.Register<IOffsetStore, InMemoryOffsetStore>();
            ObjectContainer.Register<IMessageStore, InMemoryMessageStore>();
            ObjectContainer.Register<IMessageService, MessageService>();
            return configuration;
        }
    }
}
