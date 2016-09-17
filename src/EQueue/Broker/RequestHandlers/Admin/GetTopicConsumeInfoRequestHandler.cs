using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Client;
using EQueue.Broker.Exceptions;
using EQueue.Protocols.Brokers.Requests;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers.Admin
{
    public class GetTopicConsumeInfoRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private GetTopicConsumeInfoListService _getTopicConsumeInfoListService;

        public GetTopicConsumeInfoRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _getTopicConsumeInfoListService = ObjectContainer.Resolve<GetTopicConsumeInfoListService>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (BrokerController.Instance.IsCleaning)
            {
                throw new BrokerCleanningException();
            }

            var request = _binarySerializer.Deserialize<GetTopicConsumeInfoRequest>(remotingRequest.Body);
            var topicConsumeInfoList = _getTopicConsumeInfoListService.GetTopicConsumeInfoList(request.GroupName, request.Topic);

            return RemotingResponseFactory.CreateResponse(remotingRequest, _binarySerializer.Serialize(topicConsumeInfoList));
        }
    }
}
