using System.Net;
using ECommon.Socketing;

namespace EQueue.Clients.Producers
{
    public class ProducerSetting
    {
        public IPEndPoint BrokerAddress { get; set; }
        public IPEndPoint LocalAddress { get; set; }
        public int UpdateTopicQueueCountInterval { get; set; }

        public ProducerSetting()
        {
            BrokerAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5000);
            UpdateTopicQueueCountInterval = 1000 * 5;
        }
    }
}
