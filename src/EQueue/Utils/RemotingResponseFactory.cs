using System;
using ECommon.Remoting;
using EQueue.Protocols;

namespace EQueue.Utils
{
    public class RemotingResponseFactory
    {
        private static readonly byte[] EmptyData = new byte[0];

        public static RemotingResponse CreateResponse(RemotingRequest remotingRequest)
        {
            return CreateResponse(remotingRequest, ResponseCode.Success, EmptyData);
        }
        public static RemotingResponse CreateResponse(RemotingRequest remotingRequest, short responseCode)
        {
            return CreateResponse(remotingRequest, responseCode, EmptyData);
        }
        public static RemotingResponse CreateResponse(RemotingRequest remotingRequest, byte[] data)
        {
            return CreateResponse(remotingRequest, ResponseCode.Success, data);
        }
        public static RemotingResponse CreateResponse(RemotingRequest remotingRequest, short responseCode, byte[] data)
        {
            return new RemotingResponse(
                remotingRequest.Type,
                remotingRequest.Code,
                remotingRequest.Sequence,
                remotingRequest.CreatedTime,
                responseCode,
                data,
                DateTime.Now,
                remotingRequest.Header,
                null);
        }
    }
}
