using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Client;
using EQueue.Protocols;

namespace EQueue.Broker.RequestHandlers
{
    public class ConsumerHeartbeatRequestHandler : IRequestHandler
    {
        private ConsumerManager _consumerManager;
        private IBinarySerializer _binarySerializer;

        public ConsumerHeartbeatRequestHandler(BrokerController brokerController)
        {
            _consumerManager = ObjectContainer.Resolve<ConsumerManager>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var consumerData = _binarySerializer.Deserialize<ConsumerData>(remotingRequest.Body);
            _consumerManager.RegisterConsumer(consumerData.GroupName, consumerData.ConsumerId, consumerData.SubscriptionTopics, consumerData.ConsumingQueues, context.Connection);
            return null;
        }
    }
}
