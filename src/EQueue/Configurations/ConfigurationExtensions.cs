using ECommon.Configurations;
using EQueue.Broker;
using EQueue.Broker.Client;
using EQueue.Broker.DeleteMessageStrategies;
using EQueue.Broker.LongPolling;
using EQueue.Clients.Consumers;
using EQueue.Clients.Producers;

namespace EQueue.Configurations
{
    public static class ConfigurationExtensions
    {
        public static Configuration RegisterEQueueComponents(this Configuration configuration)
        {
            configuration.SetDefault<IDeleteMessageStrategy, DeleteMessageByTimeStrategy>();
            configuration.SetDefault<IAllocateMessageQueueStrategy, AverageAllocateMessageQueueStrategy>();
            configuration.SetDefault<IQueueSelector, QueueHashSelector>();
            configuration.SetDefault<IMessageStore, DefaultMessageStore>();
            configuration.SetDefault<IQueueStore, DefaultQueueStore>();
            configuration.SetDefault<ConsumerManager, ConsumerManager>();
            configuration.SetDefault<IOffsetStore, InMemoryOffsetManager>();
            configuration.SetDefault<IQueueStore, DefaultQueueStore>();
            configuration.SetDefault<SuspendedPullRequestManager, SuspendedPullRequestManager>();

            return configuration;
        }

        public static Configuration UseDeleteMessageByTimeStrategy(this Configuration configuration, int maxStorageHours = 24 * 30)
        {
            configuration.SetDefault<IDeleteMessageStrategy, DeleteMessageByTimeStrategy>(new DeleteMessageByTimeStrategy(maxStorageHours));
            return configuration;
        }
        public static Configuration UseDeleteMessageByCountStrategy(this Configuration configuration, int maxChunkCount = 200)
        {
            configuration.SetDefault<IDeleteMessageStrategy, DeleteMessageByCountStrategy>(new DeleteMessageByCountStrategy(maxChunkCount));
            return configuration;
        }
        public static Configuration UseSqlServerOffsetManager(this Configuration configuration, SqlServerOffsetManagerSetting setting)
        {
            configuration.SetDefault<IOffsetStore, DefaultOffsetStore>(new DefaultOffsetStore(setting));
            return configuration;
        }
    }
}
