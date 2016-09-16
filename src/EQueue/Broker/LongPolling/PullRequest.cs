using System;
using ECommon.Remoting;
using EQueue.Protocols.Brokers.Requests;

namespace EQueue.Broker.LongPolling
{
    public class PullRequest
    {
        public RemotingRequest RemotingRequest { get; private set; }
        public PullMessageRequest PullMessageRequest { get; private set; }
        public IRequestHandlerContext RequestHandlerContext { get; private set; }
        public DateTime SuspendStartTime { get; private set; }
        public long SuspendMilliseconds { get; private set; }
        public Action<PullRequest> NewMessageArrivedAction { get; private set; }
        public Action<PullRequest> TimeoutAction { get; private set; }
        public Action<PullRequest> NoNewMessageAction { get; private set; }
        public Action<PullRequest> ReplacedAction { get; private set; }

        public PullRequest(
            RemotingRequest remotingRequest,
            PullMessageRequest pullMessageRequest,
            IRequestHandlerContext requestHandlerContext,
            DateTime suspendStartTime,
            long suspendMilliseconds,
            Action<PullRequest> newMessageArrivedAction,
            Action<PullRequest> timeoutAction,
            Action<PullRequest> noNewMessageAction,
            Action<PullRequest> replacedAction)
        {
            RemotingRequest = remotingRequest;
            PullMessageRequest = pullMessageRequest;
            RequestHandlerContext = requestHandlerContext;
            SuspendStartTime = suspendStartTime;
            SuspendMilliseconds = suspendMilliseconds;
            NewMessageArrivedAction = newMessageArrivedAction;
            TimeoutAction = timeoutAction;
            NoNewMessageAction = noNewMessageAction;
            ReplacedAction = replacedAction;
        }

        public bool IsTimeout()
        {
            return (DateTime.Now - SuspendStartTime).TotalMilliseconds >= SuspendMilliseconds;
        }
    }
}
