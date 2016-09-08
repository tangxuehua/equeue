using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.NameServer.RequestHandlers
{
    public class UnregisterBrokerRequestHandler : IRequestHandler
    {
        private RouteInfoManager _routeInfoManager;
        private IBinarySerializer _binarySerializer;

        public UnregisterBrokerRequestHandler(NameServerController nameServerController)
        {
            _routeInfoManager = nameServerController.RouteInfoManager;
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<BrokerUnRegistrationRequest>(remotingRequest.Body);
            _routeInfoManager.UnregisterBroker(request);
            return RemotingResponseFactory.CreateResponse(remotingRequest);
        }
    }
}
