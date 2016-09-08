using System.Collections.Generic;
using System.Net;
using ECommon.Socketing;

namespace EQueue.Clients.Producers
{
    public class ProducerSetting
    {
        /// <summary>Producer所在的集群名，一个集群下有可以有多个Producer；默认为DefaultCluster
        /// </summary>
        public string ClusterName { get; set; }
        /// <summary>NameServer地址列表
        /// </summary>
        public IEnumerable<IPEndPoint> NameServerList { get; set; }
        /// <summary>Socket通信层相关的设置；
        /// </summary>
        public SocketSetting SocketSetting { get; set; }
        /// <summary>刷新Broker信息和Topic路由信息的间隔，默认为5s；
        /// </summary>
        public int RefreshBrokerAndTopicRouteInfoInterval { get; set; }
        /// <summary>向Broker发送心跳的间隔，默认为1s；
        /// </summary>
        public int HeartbeatBrokerInterval { get; set; }
        /// <summary>消息最大允许的字节数，默认为4MB；
        /// </summary>
        public int MessageMaxSize { get; set; }
        /// <summary>发送消息遇到错误时自动重试的最大次数，默认为5次；
        /// </summary>
        public int SendMessageMaxRetryCount { get; set; }

        public ProducerSetting()
        {
            ClusterName = "DefaultCluster";
            NameServerList = new List<IPEndPoint>()
            {
                new IPEndPoint(SocketUtils.GetLocalIPV4(), 9493)
            };
            SocketSetting = new SocketSetting();
            RefreshBrokerAndTopicRouteInfoInterval = 1000 * 5;
            HeartbeatBrokerInterval = 1000;
            MessageMaxSize = 1024 * 1024 * 4;
            SendMessageMaxRetryCount = 5;
        }
    }
}
