using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.LongPolling;
using EQueue.Protocols.Brokers.Requests;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers
{
    public class SetQueueNextConsumeOffsetRequestHandler : IRequestHandler
    {
        private readonly IConsumeOffsetStore _offsetStore;
        private readonly IBinarySerializer _binarySerializer;
        private readonly SuspendedPullRequestManager _suspendedPullRequestManager;

        public SetQueueNextConsumeOffsetRequestHandler()
        {
            _offsetStore = ObjectContainer.Resolve<IConsumeOffsetStore>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _suspendedPullRequestManager = ObjectContainer.Resolve<SuspendedPullRequestManager>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (BrokerController.Instance.IsCleaning)
            {
                return null;
            }

            var request = _binarySerializer.Deserialize<SetQueueNextConsumeOffsetRequest>(remotingRequest.Body);
            _offsetStore.SetConsumeNextOffset(
                request.Topic,
                request.QueueId,
                request.ConsumerGroup,
                request.NextOffset);
            _suspendedPullRequestManager.RemovePullRequest(request.ConsumerGroup, request.Topic, request.QueueId);

            return RemotingResponseFactory.CreateResponse(remotingRequest);
        }
    }
}
