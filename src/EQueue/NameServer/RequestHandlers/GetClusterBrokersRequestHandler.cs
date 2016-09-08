using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.NameServer.RequestHandlers
{
    public class GetClusterBrokersRequestHandler : IRequestHandler
    {
        private RouteInfoManager _routeInfoManager;
        private IBinarySerializer _binarySerializer;

        public GetClusterBrokersRequestHandler(NameServerController nameServerController)
        {
            _routeInfoManager = nameServerController.RouteInfoManager;
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<GetClusterBrokersRequest>(remotingRequest.Body);
            var brokerInfoList = _routeInfoManager.GetClusterBrokers(request);
            var data = _binarySerializer.Serialize(brokerInfoList);
            return RemotingResponseFactory.CreateResponse(remotingRequest, data);
        }
    }
}
