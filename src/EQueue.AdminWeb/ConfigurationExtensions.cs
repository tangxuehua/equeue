using System.Reflection;
using Autofac.Integration.Mvc;
using ECommon.Autofac;
using ECommon.Components;
using ECommon.Configurations;

namespace EQueue.AdminWeb
{
    public static class ConfigurationExtensions
    {
        /// <summary>Register all the controllers into ioc container.
        /// </summary>
        /// <returns></returns>
        public static Configuration RegisterControllers(this Configuration configuration)
        {
            var containerBuilder = (ObjectContainer.Current as AutofacObjectContainer).ContainerBuilder;
            containerBuilder.RegisterControllers(Assembly.GetExecutingAssembly());
            return configuration;
        }
        /// <summary>Register all the services into ioc container.
        /// </summary>
        /// <returns></returns>
        public static Configuration RegisterServices(this Configuration configuration)
        {
            configuration.SetDefault<SendEmailService, SendEmailService>();
            configuration.SetDefault<MessageService, MessageService>();
            return configuration;
        }
    }
}