using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.NameServer.RequestHandlers
{
    public class RegisterBrokerRequestHandler : IRequestHandler
    {
        private RouteInfoManager _routeInfoManager;
        private IBinarySerializer _binarySerializer;

        public RegisterBrokerRequestHandler(NameServerController nameServerController)
        {
            _routeInfoManager = nameServerController.RouteInfoManager;
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<BrokerRegistrationRequest>(remotingRequest.Body);
            _routeInfoManager.RegisterBroker(request);
            return RemotingResponseFactory.CreateResponse(remotingRequest);
        }
    }
}
