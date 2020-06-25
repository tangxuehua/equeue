using System;
using System.Collections.Generic;
using System.Net;
using ECommon.Socketing;
using Microsoft.Extensions.Configuration;

namespace EQueue.AdminWeb
{
    public class EQueueSettingService
    {
        public IEnumerable<IPEndPoint> NameServerList { get; set; }
        public bool EnableMonitorMessageAccumulate { get; set; }
        public int MessageAccumulateThreshold { get; set; }
        public int ScanMessageAccumulateIntervalSeconds { get; set; }
        public SocketSetting SocketSetting { get; set; }

        public EQueueSettingService(IConfiguration configuration)
        {
            var setting = new EQueueSetting();
            configuration.GetSection("EQueueSetting").Bind(setting);

            var nameServerAddresses = setting.NameServerAddresses;
            if (string.IsNullOrWhiteSpace(nameServerAddresses))
            {
                var defaultNameServer = new IPEndPoint(SocketUtils.GetLocalIPV4(), 9493);
                var defaultList = new List<IPEndPoint>
                {
                    defaultNameServer
                };
                NameServerList = defaultList;
            }
            else
            {
                var addressList = nameServerAddresses.Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries);
                var endpointList = new List<IPEndPoint>();
                foreach (var address in addressList)
                {
                    var array = address.Split(new string[] { ":" }, StringSplitOptions.RemoveEmptyEntries);
                    var endpoint = new IPEndPoint(IPAddress.Parse(array[0]), int.Parse(array[1]));
                    endpointList.Add(endpoint);
                }
                NameServerList = endpointList;
            }

            EnableMonitorMessageAccumulate = setting.EnableMonitorMessageAccumulate;
            MessageAccumulateThreshold = setting.MessageAccumulateThreshold;
            ScanMessageAccumulateIntervalSeconds = setting.ScanMessageAccumulateIntervalSeconds;
            SocketSetting = new SocketSetting();
        }
    }

    public class EQueueSetting
    {
        public string NameServerAddresses { get; set; }
        public bool EnableMonitorMessageAccumulate { get; set; }
        public int MessageAccumulateThreshold { get; set; }
        public int ScanMessageAccumulateIntervalSeconds { get; set; }
    }
}