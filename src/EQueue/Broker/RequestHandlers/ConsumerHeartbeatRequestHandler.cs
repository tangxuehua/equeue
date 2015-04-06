using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Client;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class ConsumerHeartbeatRequestHandler : IRequestHandler
    {
        private ConsumerManager _consumerManager;
        private IBinarySerializer _binarySerializer;
        private ILogger _logger;

        public ConsumerHeartbeatRequestHandler(BrokerController brokerController)
        {
            _consumerManager = ObjectContainer.Resolve<ConsumerManager>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var consumerData = _binarySerializer.Deserialize<ConsumerData>(remotingRequest.Body);
            _consumerManager.RegisterConsumer(consumerData.GroupName, new ClientChannel(consumerData.ConsumerId, context.Channel), consumerData.SubscriptionTopics, consumerData.ConsumingQueues);
            return null;
        }
    }
}
