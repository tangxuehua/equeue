using System.Net;
using ECommon.Socketing;

namespace EQueue.NameServer
{
    public class NameServerSetting
    {
        /// <summary>服务端口号，默认为9493
        /// </summary>
        public IPEndPoint BindingAddress { get; set; }
        /// <summary>Broker不活跃最大允许时间，如果一个Broker超过此时间未发送心跳，则认为此Broker挂掉了；默认超时时间为30s;
        /// </summary>
        public int BrokerInactiveMaxMilliseconds { get; set; }
        /// <summary>是否自动创建Topic，默认为true；线上环境建议设置为false，Topic应该总是由后台管理控制台来创建；
        /// </summary>
        public bool AutoCreateTopic { get; set; }
        /// <summary>TCP通行层设置
        /// </summary>
        public SocketSetting SocketSetting { get; set; }

        public NameServerSetting(int port = 9493)
        {
            BindingAddress = new IPEndPoint(SocketUtils.GetLocalIPV4(), port);
            BrokerInactiveMaxMilliseconds = 30 * 1000;
            AutoCreateTopic = true;
        }
    }
}
