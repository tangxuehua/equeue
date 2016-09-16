using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols.NameServers.Requests;
using EQueue.Utils;

namespace EQueue.NameServer.RequestHandlers
{
    public class GetTopicQueueInfoRequestHandler : IRequestHandler
    {
        private ClusterManager _clusterManager;
        private IBinarySerializer _binarySerializer;

        public GetTopicQueueInfoRequestHandler(NameServerController nameServerController)
        {
            _clusterManager = nameServerController.ClusterManager;
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<GetTopicQueueInfoRequest>(remotingRequest.Body);
            var topicQueueInfoList = _clusterManager.GetTopicQueueInfo(request);
            var data = _binarySerializer.Serialize(topicQueueInfoList);
            return RemotingResponseFactory.CreateResponse(remotingRequest, data);
        }
    }
}
