using System;

namespace EQueue.Remoting
{
    public class ResponseFuture
    {
        private long _timeoutMillis;
        private DateTime _beginTime;

        public RemotingResponse Response { get; set; }
        public long RequestSequence { get; private set; }
        public Action<RemotingResponse> RequestCompleteCallback { get; private set; }

        public ResponseFuture(long requestSequence, long timeoutMillis, Action<RemotingResponse> requestCompleteCallback)
        {
            RequestSequence = requestSequence;
            _beginTime = DateTime.Now;
            _timeoutMillis = timeoutMillis;
            RequestCompleteCallback = requestCompleteCallback;
        }

        public bool IsTimeout()
        {
            return (DateTime.Now - _beginTime).TotalMilliseconds > _timeoutMillis;
        }
    }
}
