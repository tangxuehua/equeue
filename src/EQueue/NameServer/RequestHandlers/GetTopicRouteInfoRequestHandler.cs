using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols.NameServers.Requests;
using EQueue.Utils;

namespace EQueue.NameServer.RequestHandlers
{
    public class GetTopicRouteInfoRequestHandler : IRequestHandler
    {
        private ClusterManager _clusterManager;
        private IBinarySerializer _binarySerializer;

        public GetTopicRouteInfoRequestHandler(NameServerController nameServerController)
        {
            _clusterManager = nameServerController.ClusterManager;
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<GetTopicRouteInfoRequest>(remotingRequest.Body);
            _clusterManager.GetTopicRouteInfo(request).ContinueWith(t =>
            {
                var routeInfoList = t.Result; 
                var data = _binarySerializer.Serialize(routeInfoList);
                var response = RemotingResponseFactory.CreateResponse(remotingRequest, data);
                context.SendRemotingResponse(response);
            });
            return null;
        }
    }
}
