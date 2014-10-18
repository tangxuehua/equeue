using System.Net;
using ECommon.Utilities;

namespace EQueue.AdminWeb
{
    public class Settings
    {
        public static IPAddress BrokerAddress = SocketUtils.GetLocalIPV4();
        public static int BrokerPort = 5002;
    }
}