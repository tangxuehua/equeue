using System;
using EQueue.Remoting;
using EQueue.Remoting.Requests;

namespace EQueue.Broker.LongPolling
{
    public class PullRequest
    {
        public long RemotingRequestSequence { get; private set; }
        public PullMessageRequest PullMessageRequest { get; private set; }
        public IRequestHandlerContext RequestHandlerContext { get; private set; }
        public DateTime SuspendTime { get; private set; }
        public long SuspendMilliseconds { get; private set; }
        public Action<PullRequest> NewMessageArrivedAction { get; private set; }
        public Action<PullRequest> SuspendTimeoutAction { get; private set; }

        public PullRequest(
            long remotingRequestSequence,
            PullMessageRequest pullMessageRequest,
            IRequestHandlerContext requestHandlerContext,
            DateTime suspendTime,
            long suspendMilliseconds,
            Action<PullRequest> newMessageArrivedAction,
            Action<PullRequest> suspendTimeoutAction)
        {
            RemotingRequestSequence = remotingRequestSequence;
            PullMessageRequest = pullMessageRequest;
            RequestHandlerContext = requestHandlerContext;
            SuspendTime = suspendTime;
            SuspendMilliseconds = suspendMilliseconds;
            NewMessageArrivedAction = newMessageArrivedAction;
            SuspendTimeoutAction = suspendTimeoutAction;
        }
    }
}
