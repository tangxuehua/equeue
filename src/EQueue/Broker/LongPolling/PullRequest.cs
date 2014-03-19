using System;
using ECommon.Remoting;
using EQueue.Protocols;

namespace EQueue.Broker.LongPolling
{
    public class PullRequest
    {
        public long RemotingRequestSequence { get; private set; }
        public PullMessageRequest PullMessageRequest { get; private set; }
        public IRequestHandlerContext RequestHandlerContext { get; private set; }
        public DateTime SuspendStartTime { get; private set; }
        public long SuspendMilliseconds { get; private set; }
        public Action<PullRequest> NewMessageArrivedAction { get; private set; }
        public Action<PullRequest> TimeoutAction { get; private set; }
        public Action<PullRequest> ReplacedAction { get; private set; }

        public PullRequest(
            long remotingRequestSequence,
            PullMessageRequest pullMessageRequest,
            IRequestHandlerContext requestHandlerContext,
            DateTime suspendStartTime,
            long suspendMilliseconds,
            Action<PullRequest> newMessageArrivedAction,
            Action<PullRequest> timeoutAction,
            Action<PullRequest> replacedAction)
        {
            RemotingRequestSequence = remotingRequestSequence;
            PullMessageRequest = pullMessageRequest;
            RequestHandlerContext = requestHandlerContext;
            SuspendStartTime = suspendStartTime;
            SuspendMilliseconds = suspendMilliseconds;
            NewMessageArrivedAction = newMessageArrivedAction;
            TimeoutAction = timeoutAction;
            ReplacedAction = replacedAction;
        }

        public bool IsTimeout()
        {
            return DateTime.Now > SuspendStartTime.AddMilliseconds(SuspendMilliseconds);
        }
    }
}
