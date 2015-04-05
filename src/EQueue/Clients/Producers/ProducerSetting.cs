using System.Net;
using ECommon.Utilities;

namespace EQueue.Clients.Producers
{
    public class ProducerSetting
    {
        public IPEndPoint BrokerProducerIPEndPoint { get; set; }
        public int UpdateTopicQueueCountInterval { get; set; }

        public ProducerSetting()
        {
            BrokerProducerIPEndPoint = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5000);
            UpdateTopicQueueCountInterval = 1000 * 10;
        }
    }
}
