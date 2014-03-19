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
            configuration.SetDefault<ILocalOffsetStore, InMemoryLocalOffsetStore>();
            configuration.SetDefault<IMessageStore, InMemoryMessageStore>();
            configuration.SetDefault<IMessageService, MessageService>();
            configuration.SetDefault<IOffsetManager, InMemoryOffsetManager>();
            return configuration;
        }
        public static Configuration UseDefaultRemoteBrokerOffsetStore(this Configuration configuration)
        {
            var consumerSetting = new ConsumerSetting();
            return configuration.UseDefaultRemoteBrokerOffsetStore(consumerSetting.BrokerAddress, consumerSetting.BrokerPort);
        }
        public static Configuration UseDefaultRemoteBrokerOffsetStore(this Configuration configuration, string brokerAddress, int brokerPort)
        {
            var offsetStore = new DefaultRemoteBrokerOffsetStore(brokerAddress, brokerPort);
            configuration.SetDefault<IRemoteBrokerOffsetStore, DefaultRemoteBrokerOffsetStore>(offsetStore);
            return configuration;
        }
    }
}
