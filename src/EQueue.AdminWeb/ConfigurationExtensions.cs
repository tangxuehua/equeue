using Autofac;
using ECommon.Autofac;
using ECommon.Components;
using ECommon.Configurations;
using Microsoft.AspNetCore.Mvc;
using System.Linq;
using System.Reflection;

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

            var controllerType = typeof(Controller);
            containerBuilder.RegisterAssemblyTypes(Assembly.GetExecutingAssembly())
                .Where(x => controllerType.IsAssignableFrom(x) && x != controllerType)
                .PropertiesAutowired();

            return configuration;
        }
        /// <summary>Register all the services into ioc container.
        /// </summary>
        /// <returns></returns>
        public static Configuration RegisterServices(this Configuration configuration)
        {
            configuration.SetDefault<EQueueSettingService, EQueueSettingService>();
            configuration.SetDefault<SendEmailService, SendEmailService>();
            configuration.SetDefault<MessageService, MessageService>();
            return configuration;
        }
    }
}