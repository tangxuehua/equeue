using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class QueryBrokerStatisticInfoRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;

        public QueryBrokerStatisticInfoRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var statisticInfo = BrokerController.Instance.GetBrokerStatisticInfo();
            return new RemotingResponse((int)ResponseCode.Success, remotingRequest.Sequence, _binarySerializer.Serialize(statisticInfo));
        }
    }
}
