using System.Collections.Generic;
using System.Linq;
using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Exceptions;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers.Admin
{
    public class QueryTopicQueueInfoRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private IQueueStore _queueStore;
        private IConsumeOffsetStore _offsetStore;

        public QueryTopicQueueInfoRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _queueStore = ObjectContainer.Resolve<IQueueStore>();
            _offsetStore = ObjectContainer.Resolve<IConsumeOffsetStore>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (BrokerController.Instance.IsCleaning)
            {
                throw new BrokerCleanningException();
            }

            var request = _binarySerializer.Deserialize<QueryTopicQueueInfoRequest>(remotingRequest.Body);
            var topicQueueInfoList = new List<TopicQueueInfo>();
            var queues = _queueStore.QueryQueues(request.Topic).ToList().OrderBy(x => x.Topic).ThenBy(x => x.QueueId);

            foreach (var queue in queues)
            {
                var topicQueueInfo = new TopicQueueInfo();
                topicQueueInfo.Topic = queue.Topic;
                topicQueueInfo.QueueId = queue.QueueId;
                topicQueueInfo.QueueCurrentOffset = queue.NextOffset - 1;
                topicQueueInfo.QueueMinOffset = queue.GetMinQueueOffset();
                topicQueueInfo.QueueMinConsumedOffset = _offsetStore.GetMinConsumedOffset(queue.Topic, queue.QueueId);
                topicQueueInfo.ProducerVisible = queue.Setting.ProducerVisible;
                topicQueueInfo.ConsumerVisible = queue.Setting.ConsumerVisible;
                topicQueueInfoList.Add(topicQueueInfo);
            }

            return RemotingResponseFactory.CreateResponse(remotingRequest, _binarySerializer.Serialize(topicQueueInfoList));
        }
    }
}
