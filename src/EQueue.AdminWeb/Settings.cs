using System.Net;
using ECommon.Socketing;
using ECommon.Utilities;

namespace EQueue.AdminWeb
{
    public class Settings
    {
        public static IPEndPoint BrokerAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5002);
    }
}