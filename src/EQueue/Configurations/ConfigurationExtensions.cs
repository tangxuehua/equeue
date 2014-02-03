using ECommon.Configurations;
using ECommon.IoC;
using EQueue.Broker;
using EQueue.Clients.Consumers;
using EQueue.Clients.Consumers.OffsetStores;
using EQueue.Clients.Producers;

namespace EQueue.Configurations
{
    public static class ConfigurationExtensions
    {
        public static Configuration RegisterEQueueComponents(this Configuration configuration)
        {
            ObjectContainer.Register<IAllocateMessageQueueStrategy, AverageAllocateMessageQueueStrategy>();
            ObjectContainer.Register<IQueueSelector, QueueHashSelector>();
            ObjectContainer.Register<ILocalOffsetStore, DefaultLocalOffsetStore>();
            ObjectContainer.Register<IRemoteBrokerOffsetStore, DefaultRemoteBrokerOffsetStore>();
            ObjectContainer.Register<IMessageStore, InMemoryMessageStore>();
            ObjectContainer.Register<IMessageService, MessageService>();
            return configuration;
        }
    }
}
