using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Client;
using EQueue.Protocols.Brokers;
using System.Text;

namespace EQueue.Broker.RequestHandlers
{
    public class ConsumerHeartbeatRequestHandler : IRequestHandler
    {
        private ConsumerManager _consumerManager;
        private IJsonSerializer _jsonSerializer;

        public ConsumerHeartbeatRequestHandler(BrokerController brokerController)
        {
            _consumerManager = ObjectContainer.Resolve<ConsumerManager>();
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var consumerData = _jsonSerializer.Deserialize<ConsumerHeartbeatData>(Encoding.UTF8.GetString(remotingRequest.Body));
            _consumerManager.RegisterConsumer(consumerData.GroupName, consumerData.ConsumerId, consumerData.SubscriptionTopics, consumerData.ConsumingQueues, context.Connection);
            return null;
        }
    }
}
