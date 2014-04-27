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
        private BrokerController _brokerController;
        private IBinarySerializer _binarySerializer;
        private ILogger _logger;

        public ConsumerHeartbeatRequestHandler(BrokerController brokerController)
        {
            _brokerController = brokerController;
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest request)
        {
            var consumerData = _binarySerializer.Deserialize<ConsumerData>(request.Body);
            _brokerController.ConsumerManager.RegisterConsumer(
                consumerData.GroupName,
                new ClientChannel(consumerData.ConsumerId, context.Channel), consumerData.SubscriptionTopics);
            return null;
        }
    }
}
