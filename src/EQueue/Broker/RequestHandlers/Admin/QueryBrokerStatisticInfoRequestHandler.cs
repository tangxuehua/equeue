using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers.Admin
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
            return RemotingResponseFactory.CreateResponse(remotingRequest, _binarySerializer.Serialize(statisticInfo));
        }
    }
}
