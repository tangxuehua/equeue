using EQueue.Broker.Client;
using EQueue.Infrastructure;
using EQueue.Infrastructure.IoC;
using EQueue.Infrastructure.Logging;
using EQueue.Protocols;
using EQueue.Remoting;

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
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest request)
        {
            var consumerData = _binarySerializer.Deserialize<ConsumerData>(request.Body);
            _brokerController.ConsumerManager.RegisterConsumer(
                consumerData.GroupName,
                new ClientChannel(consumerData.ConsumerId, context.Channel),
                consumerData.MessageModel,
                consumerData.SubscriptionTopics);
            //_logger.InfoFormat("Handled ConsumerHeartbeatRequest. consumerData:{0}, channel:{1}", consumerData, context.Channel);
            return null;
        }
    }
}
