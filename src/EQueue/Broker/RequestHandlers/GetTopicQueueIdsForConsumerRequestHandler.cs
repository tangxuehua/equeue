using System.Linq;
using System.Text;
using ECommon.Components;
using ECommon.Remoting;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers
{
    public class GetTopicQueueIdsForConsumerRequestHandler : IRequestHandler
    {
        private IQueueStore _queueStore;

        public GetTopicQueueIdsForConsumerRequestHandler()
        {
            _queueStore = ObjectContainer.Resolve<IQueueStore>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var topic = Encoding.UTF8.GetString(remotingRequest.Body);
            var queueIds = _queueStore.GetQueues(topic).Where(x => x.Setting.ConsumerVisible).Select(x => x.QueueId).ToList();
            var data = Encoding.UTF8.GetBytes(string.Join(",", queueIds));
            return RemotingResponseFactory.CreateResponse(remotingRequest, data);
        }
    }
}
