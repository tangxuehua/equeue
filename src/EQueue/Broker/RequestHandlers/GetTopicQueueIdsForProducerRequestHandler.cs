using System.Text;
using ECommon.Components;
using ECommon.Remoting;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class GetTopicQueueIdsForProducerRequestHandler : IRequestHandler
    {
        private IMessageService _messageService;

        public GetTopicQueueIdsForProducerRequestHandler()
        {
            _messageService = ObjectContainer.Resolve<IMessageService>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var topic = Encoding.UTF8.GetString(remotingRequest.Body);
            var queueIds = _messageService.GetQueueIdsForProducer(topic);
            var data = Encoding.UTF8.GetBytes(string.Join(",", queueIds));
            return new RemotingResponse((int)ResponseCode.Success, remotingRequest.Sequence, data);
        }
    }
}
