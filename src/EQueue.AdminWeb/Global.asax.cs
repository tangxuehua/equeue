using System.Web;
using System.Web.Mvc;
using System.Web.Routing;
using Autofac.Integration.Mvc;
using ECommon.Autofac;
using ECommon.Components;
using ECommon.Configurations;

namespace EQueue.AdminWeb
{
    public class MvcApplication : HttpApplication
    {
        protected void Application_Start()
        {
            AreaRegistration.RegisterAllAreas();
            FilterConfig.RegisterGlobalFilters(GlobalFilters.Filters);
            RouteConfig.RegisterRoutes(RouteTable.Routes);

            var configuration = Configuration
                .Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4Net()
                .UseJsonNet()
                .RegisterControllers()
                .RegisterServices()
                .BuildContainer();
            var container = (ObjectContainer.Current as AutofacObjectContainer).Container;
            DependencyResolver.SetResolver(new AutofacDependencyResolver(container));
            ObjectContainer.Resolve<MessageService>().Start();
        }
    }
}
