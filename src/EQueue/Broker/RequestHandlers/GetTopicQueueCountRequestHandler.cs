using System;
using System.Text;
using ECommon.IoC;
using ECommon.Logging;
using ECommon.Remoting;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class GetTopicQueueCountRequestHandler : IRequestHandler
    {
        private IMessageService _messageService;
        private ILogger _logger;

        public GetTopicQueueCountRequestHandler()
        {
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
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
