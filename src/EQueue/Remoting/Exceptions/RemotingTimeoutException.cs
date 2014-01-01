using System;

namespace EQueue.Remoting.Exceptions
{
    public class RemotingTimeoutException : Exception
    {
        public RemotingTimeoutException(string address, RemotingRequest request, int timeoutMillis)
            : base(string.Format("Wait response on the channel <{0}> timeout, request [{1}], {2}ms", address, request, timeoutMillis))
        {
        }
    }
}
