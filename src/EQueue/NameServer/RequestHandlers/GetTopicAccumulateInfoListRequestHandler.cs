using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols.NameServers.Requests;
using EQueue.Utils;

namespace EQueue.NameServer.RequestHandlers
{
    public class GetTopicAccumulateInfoListRequestHandler : IRequestHandler
    {
        private ClusterManager _clusterManager;
        private IBinarySerializer _binarySerializer;

        public GetTopicAccumulateInfoListRequestHandler(NameServerController nameServerController)
        {
            _clusterManager = nameServerController.ClusterManager;
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<GetTopicAccumulateInfoListRequest>(remotingRequest.Body);
            var topicAccumulateInfoList = _clusterManager.GetTopicAccumulateInfoList(request);
            var data = _binarySerializer.Serialize(topicAccumulateInfoList);
            return RemotingResponseFactory.CreateResponse(remotingRequest, data);
        }
    }
}
