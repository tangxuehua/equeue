using System.Net;
using ECommon.Utilities;

namespace EQueue.Utils
{
    public static class ClientIdFactory
    {
        public static string CreateClientId(IPEndPoint clientEndPoint)
        {
            Ensure.NotNull(clientEndPoint, "clientEndPoint");
            return string.Format("{0}@{1}", clientEndPoint.Address, clientEndPoint.Port);
        }
    }
}
