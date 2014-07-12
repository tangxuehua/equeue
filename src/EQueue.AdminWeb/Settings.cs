using ECommon.Socketing;

namespace EQueue.AdminWeb
{
    public class Settings
    {
        public static string BrokerAddress = SocketUtils.GetLocalIPV4().ToString();
        public static int BrokerPort = 5002;
    }
}