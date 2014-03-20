using ECommon.Configurations;
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
            configuration.SetDefault<IAllocateMessageQueueStrategy, AverageAllocateMessageQueueStrategy>();
            configuration.SetDefault<IQueueSelector, QueueHashSelector>();
            configuration.SetDefault<ILocalOffsetStore, DefaultLocalOffsetStore>();
            configuration.SetDefault<IRemoteBrokerOffsetStore, DefaultRemoteBrokerOffsetStore>();
            configuration.SetDefault<IMessageStore, InMemoryMessageStore>();
            configuration.SetDefault<IMessageService, MessageService>();
            configuration.SetDefault<IOffsetManager, InMemoryOffsetManager>();
            return configuration;
        }
    }
}
