using ECommon.Configurations;
using EQueue.Broker;
using EQueue.Broker.Client;
using EQueue.Broker.LongPolling;
using EQueue.Broker.Storage;
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
            configuration.SetDefault<IQueueStore, InMemoryQueueStore>();
            configuration.SetDefault<IMessageStore, DefaultMessageStore>();
            configuration.SetDefault<ConsumerManager, ConsumerManager>();
            configuration.SetDefault<IOffsetManager, InMemoryOffsetManager>();
            configuration.SetDefault<IQueueService, QueueService>();
            configuration.SetDefault<IMessageService, MessageService>();
            configuration.SetDefault<SuspendedPullRequestManager, SuspendedPullRequestManager>();

            var recordParserProvider = new DefaultLogRecordParserProvider();
            recordParserProvider
                .RegisterParser(1, new MessageLogRecordParser())
                .RegisterParser(2, new QueueLogRecordParser());
            configuration.SetDefault<ILogRecordParserProvider, DefaultLogRecordParserProvider>(recordParserProvider);

            return configuration;
        }

        public static Configuration UseSqlServerQueueStore(this Configuration configuration, SqlServerQueueStoreSetting setting)
        {
            configuration.SetDefault<IQueueStore, SqlServerQueueStore>(new SqlServerQueueStore(setting));
            return configuration;
        }
        public static Configuration UseSqlServerOffsetManager(this Configuration configuration, SqlServerOffsetManagerSetting setting)
        {
            configuration.SetDefault<IOffsetManager, SqlServerOffsetManager>(new SqlServerOffsetManager(setting));
            return configuration;
        }
    }
}
