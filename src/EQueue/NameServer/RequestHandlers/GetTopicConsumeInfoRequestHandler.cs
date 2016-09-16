using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols.NameServers.Requests;
using EQueue.Utils;

namespace EQueue.NameServer.RequestHandlers
{
    public class GetTopicConsumeInfoRequestHandler : IRequestHandler
    {
        private ClusterManager _clusterManager;
        private IBinarySerializer _binarySerializer;

        public GetTopicConsumeInfoRequestHandler(NameServerController nameServerController)
        {
            _clusterManager = nameServerController.ClusterManager;
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<GetTopicConsumeInfoRequest>(remotingRequest.Body);
            var topicConsumeInfoList = _clusterManager.GetTopicConsumeInfo(request);
            var data = _binarySerializer.Serialize(topicConsumeInfoList);
            return RemotingResponseFactory.CreateResponse(remotingRequest, data);
        }
    }
}
