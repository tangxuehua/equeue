using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols.NameServers.Requests;
using EQueue.Utils;

namespace EQueue.NameServer.RequestHandlers
{
    public class GetClusterBrokersRequestHandler : IRequestHandler
    {
        private ClusterManager _clusterManager;
        private IBinarySerializer _binarySerializer;

        public GetClusterBrokersRequestHandler(NameServerController nameServerController)
        {
            _clusterManager = nameServerController.ClusterManager;
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<GetClusterBrokersRequest>(remotingRequest.Body);
            var brokerInfoList = _clusterManager.GetClusterBrokers(request);
            var data = _binarySerializer.Serialize(brokerInfoList);
            return RemotingResponseFactory.CreateResponse(remotingRequest, data);
        }
    }
}
