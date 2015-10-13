using ECommon.Components;
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

        public ConsumerHeartbeatRequestHandler(BrokerController brokerController)
        {
            _consumerManager = ObjectContainer.Resolve<ConsumerManager>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var consumerData = _binarySerializer.Deserialize<ConsumerData>(remotingRequest.Body);
            _consumerManager.RegisterConsumer(consumerData.GroupName, new ClientChannel(consumerData.ConsumerId, context.Connection), consumerData.SubscriptionTopics, consumerData.ConsumingQueues);
            return null;
        }
    }
}
