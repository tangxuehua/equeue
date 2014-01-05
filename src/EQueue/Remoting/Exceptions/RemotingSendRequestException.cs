using System;

namespace EQueue.Remoting.Exceptions
{
    public class RemotingSendRequestException : Exception
    {
        public RemotingSendRequestException(string address, RemotingRequest request, Exception exception)
            : base(string.Format("Send request {0} to <{1}> failed.", request, address), exception)
        {
        }
    }
}
