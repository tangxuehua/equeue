using System;
using System.Text;
using EQueue.Infrastructure.IoC;
using EQueue.Remoting;

namespace EQueue.Broker.Processors
{
    public class GetTopicQueueCountRequestHandler : IRequestHandler
    {
        private IMessageService _messageService;

        public GetTopicQueueCountRequestHandler()
        {
            _messageService = ObjectContainer.Resolve<IMessageService>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest request)
        {
            var topic = Encoding.UTF8.GetString(request.Body);
            var queueCount = _messageService.GetTopicQueueCount(topic);
            var data = BitConverter.GetBytes(queueCount);
            return new RemotingResponse((int)ResponseCode.Success, request.Sequence, data);
        }
    }
}
