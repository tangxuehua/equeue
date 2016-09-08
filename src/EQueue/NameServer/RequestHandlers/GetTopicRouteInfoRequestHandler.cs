using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.NameServer.RequestHandlers
{
    public class GetTopicRouteInfoRequestHandler : IRequestHandler
    {
        private RouteInfoManager _routeInfoManager;
        private IBinarySerializer _binarySerializer;

        public GetTopicRouteInfoRequestHandler(NameServerController nameServerController)
        {
            _routeInfoManager = nameServerController.RouteInfoManager;
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<GetTopicRouteInfoRequest>(remotingRequest.Body);
            var routeInfoList = _routeInfoManager.GetTopicRouteInfo(request);
            var data = _binarySerializer.Serialize(routeInfoList);
            return RemotingResponseFactory.CreateResponse(remotingRequest, data);
        }
    }
}
