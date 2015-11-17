using System.Net;
using ECommon.Socketing;

namespace EQueue.Clients.Producers
{
    public class ProducerSetting
    {
        /// <summary>Broker的发送消息的地址
        /// </summary>
        public IPEndPoint BrokerAddress { get; set; }
        /// <summary>Broker处理管理请求的地址
        /// </summary>
        public IPEndPoint BrokerAdminAddress { get; set; }
        /// <summary>本地所绑定的地址，可以为空，开发者可以指定本地所使用的端口；
        /// </summary>
        public IPEndPoint LocalAddress { get; set; }
        /// <summary>本地管理所绑定的地址，可以为空，开发者可以指定本地所使用的端口；
        /// </summary>
        public IPEndPoint LocalAdminAddress { get; set; }
        /// <summary>Socket通信层相关的设置；
        /// </summary>
        public SocketSetting SocketSetting { get; set; }
        /// <summary>从Broker获取最新队列信息的间隔，默认为1s；
        /// </summary>
        public int UpdateTopicQueueCountInterval { get; set; }
        /// <summary>向Broker发送心跳的间隔，默认为1s；
        /// </summary>
        public int HeartbeatBrokerInterval { get; set; }
        /// <summary>消息最大允许的字节数，默认为4MB；
        /// </summary>
        public int MessageMaxSize { get; set; }

        public ProducerSetting()
        {
            BrokerAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5000);
            BrokerAdminAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), 5002);
            SocketSetting = new SocketSetting();
            UpdateTopicQueueCountInterval = 1000;
            HeartbeatBrokerInterval = 1000;
            MessageMaxSize = 1024 * 1024 * 4;
        }
    }
}
