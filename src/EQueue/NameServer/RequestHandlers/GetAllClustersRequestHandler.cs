using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Utils;

namespace EQueue.NameServer.RequestHandlers
{
    public class GetAllClustersRequestHandler : IRequestHandler
    {
        private ClusterManager _clusterManager;
        private IBinarySerializer _binarySerializer;

        public GetAllClustersRequestHandler(NameServerController nameServerController)
        {
            _clusterManager = nameServerController.ClusterManager;
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var clusterList = _clusterManager.GetAllClusters();
            var data = _binarySerializer.Serialize(clusterList);
            return RemotingResponseFactory.CreateResponse(remotingRequest, data);
        }
    }
}
