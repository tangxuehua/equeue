using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Exceptions;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers.Admin
{
    public class GetBrokerStatisticInfoRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;

        public GetBrokerStatisticInfoRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (BrokerController.Instance.IsCleaning)
            {
                throw new BrokerCleanningException();
            }
            var statisticInfo = BrokerController.Instance.GetBrokerStatisticInfo();
            return RemotingResponseFactory.CreateResponse(remotingRequest, _binarySerializer.Serialize(statisticInfo));
        }
    }
}
