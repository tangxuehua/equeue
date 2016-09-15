using System.Text;
using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Client;

namespace EQueue.Broker.RequestHandlers
{
    public class ProducerHeartbeatRequestHandler : IRequestHandler
    {
        private ProducerManager _producerManager;
        private IBinarySerializer _binarySerializer;

        public ProducerHeartbeatRequestHandler(BrokerController brokerController)
        {
            _producerManager = ObjectContainer.Resolve<ProducerManager>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var producerId = Encoding.UTF8.GetString(remotingRequest.Body);
            _producerManager.RegisterProducer(context.Connection, producerId);
            return null;
        }
    }
}
