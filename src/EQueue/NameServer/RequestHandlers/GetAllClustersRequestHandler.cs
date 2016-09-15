using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Utils;

namespace EQueue.NameServer.RequestHandlers
{
    public class GetAllClustersRequestHandler : IRequestHandler
    {
        private RouteInfoManager _routeInfoManager;
        private IBinarySerializer _binarySerializer;

        public GetAllClustersRequestHandler(NameServerController nameServerController)
        {
            _routeInfoManager = nameServerController.RouteInfoManager;
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var clusterList = _routeInfoManager.GetAllClusters();
            var data = _binarySerializer.Serialize(clusterList);
            return RemotingResponseFactory.CreateResponse(remotingRequest, data);
        }
    }
}
