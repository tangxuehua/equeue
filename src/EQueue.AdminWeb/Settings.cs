using System.Configuration;
using System.Net;
using ECommon.Socketing;

namespace EQueue.AdminWeb
{
    public class Settings
    {
        public static IPEndPoint BrokerAddress;

        static Settings() {
            var brokerIp = ConfigurationManager.AppSettings["brokerIp"];

            if (string.IsNullOrWhiteSpace(brokerIp))
            {
                BrokerAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5002);
            }
            else
            {
                BrokerAddress = new IPEndPoint(IPAddress.Parse(brokerIp), 5002);
            }
        }
    }
}