using ECommon.Configurations;
using EQueue.Broker;
using EQueue.Clients.Consumers;
using EQueue.Clients.Producers;

namespace EQueue.Configurations
{
    public static class ConfigurationExtensions
    {
        public static Configuration RegisterEQueueComponents(this Configuration configuration)
        {
            configuration.SetDefault<IAllocateMessageQueueStrategy, AverageAllocateMessageQueueStrategy>();
            configuration.SetDefault<IQueueSelector, QueueHashSelector>();
            configuration.SetDefault<IMessageStore, InMemoryMessageStore>(new InMemoryMessageStore(new InMemoryMessageStoreSetting()));
            configuration.SetDefault<IMessageService, MessageService>();
            configuration.SetDefault<IOffsetManager, InMemoryOffsetManager>();
            return configuration;
        }

        public static Configuration UseInMemoryMessageStore(this Configuration configuration, InMemoryMessageStoreSetting setting)
        {
            configuration.SetDefault<IMessageStore, InMemoryMessageStore>(new InMemoryMessageStore(setting));
            return configuration;
        }
        public static Configuration UseSqlServerMessageStore(this Configuration configuration, SqlServerMessageStoreSetting setting)
        {
            configuration.SetDefault<IMessageStore, SqlServerMessageStore>(new SqlServerMessageStore(setting));
            return configuration;
        }
        public static Configuration UseSqlServerOffsetManager(this Configuration configuration, SqlServerOffsetManagerSetting setting)
        {
            configuration.SetDefault<IOffsetManager, SqlServerOffsetManager>(new SqlServerOffsetManager(setting));
            return configuration;
        }
    }
}
