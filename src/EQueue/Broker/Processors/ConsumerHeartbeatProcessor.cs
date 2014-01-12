using EQueue.Broker.Client;
using EQueue.Infrastructure;
using EQueue.Infrastructure.IoC;
using EQueue.Infrastructure.Logging;
using EQueue.Protocols;
using EQueue.Remoting;

namespace EQueue.Broker.Processors
{
    public class ConsumerHeartbeatProcessor : IRequestProcessor
    {
        private IMessageService _messageService;
        private IBinarySerializer _binarySerializer;
        private BrokerController _brokerController;
        private ILogger _logger;

        public ConsumerHeartbeatProcessor(BrokerController brokerController)
        {
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _brokerController = brokerController;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
        }

        public RemotingResponse ProcessRequest(IRequestHandlerContext context, RemotingRequest request)
        {
            var consumerData = _binarySerializer.Deserialize<ConsumerData>(request.Body);
            var changed = _brokerController.ConsumerManager.RegisterConsumer(
                consumerData.GroupName,
                new ClientChannel(consumerData.ConsumerId, context.Channel),
                consumerData.MessageModel,
                consumerData.SubscriptionTopics);
            if (changed)
            {
                _logger.InfoFormat("ConsumerGroup changed, consumerData:{0}, channel:{1}", consumerData, context.Channel.RemotingAddress);
            }
            return new RemotingResponse((int)ResponseCode.Success, request.Sequence, new byte[0]);
        }
    }
}
