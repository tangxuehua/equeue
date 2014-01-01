using System;
using System.Threading.Tasks;

namespace EQueue.Remoting
{
    public class ResponseFuture
    {
        private long _timeoutMillis;
        private DateTime _beginTime;
        private TaskCompletionSource<RemotingResponse> _requestTaskCompletionSource;

        public bool SendRequestSuccess { get; set; }
        public Exception SendException { get; set; }
        public RemotingRequest Request { get; private set; }


        public ResponseFuture(RemotingRequest request, long timeoutMillis, TaskCompletionSource<RemotingResponse> requestTaskCompletionSource)
        {
            _beginTime = DateTime.Now;
            _timeoutMillis = timeoutMillis;
            _requestTaskCompletionSource = requestTaskCompletionSource;
            SendRequestSuccess = false;
            Request = request;
        }

        public bool IsTimeout()
        {
            return (DateTime.Now - _beginTime).TotalMilliseconds > _timeoutMillis;
        }
        public void CompleteRequestTask(RemotingResponse response)
        {
            _requestTaskCompletionSource.SetResult(response);
        }
    }
}
