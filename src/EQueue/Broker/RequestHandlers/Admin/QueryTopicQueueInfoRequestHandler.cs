using System.Collections.Generic;
using System.Linq;
using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers.Admin
{
    public class QueryTopicQueueInfoRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private IQueueStore _queueService;
        private IConsumeOffsetStore _offsetStore;

        public QueryTopicQueueInfoRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _queueService = ObjectContainer.Resolve<IQueueStore>();
            _offsetStore = ObjectContainer.Resolve<IConsumeOffsetStore>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<QueryTopicQueueInfoRequest>(remotingRequest.Body);
            var topicQueueInfoList = new List<TopicQueueInfo>();
            var topicList = !string.IsNullOrEmpty(request.Topic) ? new List<string> { request.Topic } : _queueService.GetAllTopics().ToList();

            foreach (var topic in topicList)
            {
                var queues = _queueService.QueryQueues(topic).ToList();
                foreach (var queue in queues)
                {
                    var topicQueueInfo = new TopicQueueInfo();
                    topicQueueInfo.Topic = queue.Topic;
                    topicQueueInfo.QueueId = queue.QueueId;
                    topicQueueInfo.QueueCurrentOffset = queue.NextOffset - 1;
                    topicQueueInfo.QueueMinOffset = queue.GetMinQueueOffset();
                    topicQueueInfo.QueueMaxConsumedOffset = _offsetStore.GetMinConsumedOffset(queue.Topic, queue.QueueId);
                    topicQueueInfo.Status = queue.Setting.Status;
                    topicQueueInfoList.Add(topicQueueInfo);
                }
            }

            return RemotingResponseFactory.CreateResponse(remotingRequest, _binarySerializer.Serialize(topicQueueInfoList));
        }
    }
}
