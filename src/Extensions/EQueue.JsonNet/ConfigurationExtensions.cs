using EQueue.Infrastructure;

namespace EQueue.JsonNet
{
    /// <summary>EQueue configuration class JsonNet extensions.
    /// </summary>
    public static class ConfigurationExtensions
    {
        /// <summary>Use Json.Net as the json serializer for the equeue framework.
        /// </summary>
        /// <returns></returns>
        public static Configuration UseJsonNet(this Configuration configuration)
        {
            configuration.SetDefault<IJsonSerializer, NewtonsoftJsonSerializer>(new NewtonsoftJsonSerializer());
            return configuration;
        }
    }
}
