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

            SocketSetting = new SocketSetting();
        }
    }
}