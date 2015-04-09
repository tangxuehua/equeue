using System.Linq;
using System.Text;
using ECommon.Components;
using ECommon.Remoting;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class GetTopicQueueIdsForProducerRequestHandler : IRequestHandler
    {
        private IQueueService _queueService;

        public GetTopicQueueIdsForProducerRequestHandler()
        {
            _queueService = ObjectContainer.Resolve<IQueueService>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var topic = Encoding.UTF8.GetString(remotingRequest.Body);
            var queueIds = _queueService.GetOrCreateQueues(topic, QueueStatus.Normal).Select(x => x.QueueId).ToList();
            var data = Encoding.UTF8.GetBytes(string.Join(",", queueIds));
            return new RemotingResponse((int)ResponseCode.Success, remotingRequest.Sequence, data);
        }
    }
}
