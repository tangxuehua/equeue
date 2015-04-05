using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class QueryBrokerStatisticInfoRequestHandler : IRequestHandler
    {
        private IMessageService _messageService;
        private IBinarySerializer _binarySerializer;

        public QueryBrokerStatisticInfoRequestHandler()
        {
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var statisticInfo = _messageService.GetBrokerStatisticInfo();
            return new RemotingResponse((int)ResponseCode.Success, remotingRequest.Sequence, _binarySerializer.Serialize(statisticInfo));
        }
    }
}
