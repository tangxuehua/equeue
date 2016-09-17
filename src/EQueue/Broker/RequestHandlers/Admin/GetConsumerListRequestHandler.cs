using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Client;
using EQueue.Broker.Exceptions;
using EQueue.Protocols.Brokers.Requests;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers.Admin
{
    public class GetConsumerListRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private GetConsumerListService _getConsumerListService;

        public GetConsumerListRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _getConsumerListService = ObjectContainer.Resolve<GetConsumerListService>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (BrokerController.Instance.IsCleaning)
            {
                throw new BrokerCleanningException();
            }

            var request = _binarySerializer.Deserialize<GetConsumerListRequest>(remotingRequest.Body);
            var consumerInfoList = _getConsumerListService.GetConsumerList(request.GroupName, request.Topic);

            return RemotingResponseFactory.CreateResponse(remotingRequest, _binarySerializer.Serialize(consumerInfoList));
        }
    }
}
