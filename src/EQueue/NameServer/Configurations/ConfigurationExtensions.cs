using ECommon.Components;
using ECommon.Configurations;
using EQueue.Broker;
using EQueue.Broker.Client;
using EQueue.Broker.DeleteMessageStrategies;
using EQueue.Broker.LongPolling;
using EQueue.Clients.Consumers;
using EQueue.Clients.Producers;
using EQueue.Utils;

namespace EQueue.Configurations
{
    public static class ConfigurationExtensions
    {
        public static Configuration RegisterEQueueComponents(this Configuration configuration)
        {
            configuration.SetDefault<IDeleteMessageStrategy, DeleteMessageByCountStrategy>();
            configuration.SetDefault<IAllocateMessageQueueStrategy, AverageAllocateMessageQueueStrategy>();
            configuration.SetDefault<IQueueSelector, QueueHashSelector>();
            configuration.SetDefault<IMessageStore, DefaultMessageStore>();
            configuration.SetDefault<IQueueStore, DefaultQueueStore>();
            configuration.SetDefault<ProducerManager, ProducerManager>();
            configuration.SetDefault<ConsumerManager, ConsumerManager>();
            configuration.SetDefault<IConsumeOffsetStore, DefaultConsumeOffsetStore>();
            configuration.SetDefault<IQueueStore, DefaultQueueStore>();
            configuration.SetDefault<GetTopicConsumeInfoListService, GetTopicConsumeInfoListService>();
            configuration.SetDefault<GetConsumerListService, GetConsumerListService>();
            configuration.SetDefault<SuspendedPullRequestManager, SuspendedPullRequestManager>();
            configuration.SetDefault<ITpsStatisticService, DefaultTpsStatisticService>();
            configuration.SetDefault<IChunkStatisticService, DefaultChunkStatisticService>();

            return configuration;
        }

        public static Configuration UseDeleteMessageByTimeStrategy(this Configuration configuration, int maxStorageHours = 24 * 30)
        {
            configuration.SetDefault<IDeleteMessageStrategy, DeleteMessageByTimeStrategy>(new DeleteMessageByTimeStrategy(maxStorageHours));
            return configuration;
        }
        public static Configuration UseDeleteMessageByCountStrategy(this Configuration configuration, int maxChunkCount = 100)
        {
            configuration.SetDefault<IDeleteMessageStrategy, DeleteMessageByCountStrategy>(new DeleteMessageByCountStrategy(maxChunkCount));
            return configuration;
        }
    }
}
