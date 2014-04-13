using ECommon.Configurations;
using EQueue.Broker;

namespace Equeue.StsDb
{
    public static class ConfigurationExtensions
    {
        /// <summary>Use stsdb to implement the message store.
        /// </summary>
        /// <returns></returns>
        public static Configuration UseStsDbMessageStore(this Configuration configuration)
        {
            return UseStsDbMessageStore(configuration, "stsdb_message_file");
        }
        /// <summary>Use stsdb to implement the message store.
        /// </summary>
        /// <returns></returns>
        public static Configuration UseStsDbMessageStore(this Configuration configuration, string storageFileName)
        {
            configuration.SetDefault<IMessageStore, StsDbMessageStore>(new StsDbMessageStore(storageFileName));
            return configuration;
        }
    }
}
