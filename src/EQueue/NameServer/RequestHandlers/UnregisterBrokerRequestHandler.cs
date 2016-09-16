using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols.NameServers.Requests;
using EQueue.Utils;

namespace EQueue.NameServer.RequestHandlers
{
    public class UnregisterBrokerRequestHandler : IRequestHandler
    {
        private ClusterManager _clusterManager;
        private IBinarySerializer _binarySerializer;

        public UnregisterBrokerRequestHandler(NameServerController nameServerController)
        {
            _clusterManager = nameServerController.ClusterManager;
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<BrokerUnRegistrationRequest>(remotingRequest.Body);
            _clusterManager.UnregisterBroker(request);
            return RemotingResponseFactory.CreateResponse(remotingRequest);
        }
    }
}
