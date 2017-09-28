using System;
using System.Collections.Generic;
using System.Configuration;
using System.Net;
using ECommon.Socketing;

namespace EQueue.AdminWeb
{
    public class Settings
    {
        public static IEnumerable<IPEndPoint> NameServerList { get; set; }
        public static bool EnableMonitorMessageAccumulate { get; set; }
        public static int MessageAccumulateThreshold { get; set; }
        public static int ScanMessageAccumulateIntervalSeconds { get; set; }
        public static SocketSetting SocketSetting { get; set; }

        static Settings() {
            var nameServerAddresses = ConfigurationManager.AppSettings["nameServerAddresses"];

            if (string.IsNullOrWhiteSpace(nameServerAddresses))
            {
                var defaultNameServer = new IPEndPoint(SocketUtils.GetLocalIPV4(), 9493);
                var defaultList = new List<IPEndPoint>();
                defaultList.Add(defaultNameServer);
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

            var enableMonitorMessageAccumulate = ConfigurationManager.AppSettings["enableMonitorMessageAccumulate"];
            if (string.IsNullOrWhiteSpace(enableMonitorMessageAccumulate))
            {
                EnableMonitorMessageAccumulate = false;
            }
            else
            {
                EnableMonitorMessageAccumulate = bool.Parse(enableMonitorMessageAccumulate);
            }

            var messageAccumulateThreshold = ConfigurationManager.AppSettings["messageAccumulateThreshold"];
            if (string.IsNullOrWhiteSpace(messageAccumulateThreshold))
            {
                MessageAccumulateThreshold = 10 * 10000;
            }
            else
            {
                MessageAccumulateThreshold = int.Parse(messageAccumulateThreshold);
            }

            var scanMessageAccumulateIntervalSeconds = ConfigurationManager.AppSettings["scanMessageAccumulateIntervalSeconds"];
            if (string.IsNullOrWhiteSpace(scanMessageAccumulateIntervalSeconds))
            {
                ScanMessageAccumulateIntervalSeconds = 60 * 5;
            }
            else
            {
                ScanMessageAccumulateIntervalSeconds = int.Parse(scanMessageAccumulateIntervalSeconds);
            }

            SocketSetting = new SocketSetting();
        }
    }
}