using System.Linq;
using System.Net;
using System.Net.Sockets;

namespace EQueue.Infrastructure
{
    public class Utils
    {
        public static string GetLocalIPV4()
        {
            return Dns.GetHostEntry(Dns.GetHostName()).AddressList.Single(x => x.AddressFamily == AddressFamily.InterNetwork).ToString();
        }
    }
}
