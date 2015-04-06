using System.Linq;
using System.Text;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class GetTopicQueueIdsForConsumerRequestHandler : IRequestHandler
    {
        private IQueueService _queueService;

        public GetTopicQueueIdsForConsumerRequestHandler()
        {
            _queueService = ObjectContainer.Resolve<IQueueService>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var topic = Encoding.UTF8.GetString(remotingRequest.Body);
            var queueIds = _queueService.FindQueues(topic).Select(x => x.QueueId).ToList();
            var data = Encoding.UTF8.GetBytes(string.Join(",", queueIds));
            return new RemotingResponse((int)ResponseCode.Success, remotingRequest.Sequence, data);
        }
    }
}
