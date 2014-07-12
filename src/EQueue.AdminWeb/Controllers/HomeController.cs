using System.Web.Mvc;

namespace EQueue.AdminWeb.Controllers
{
    public class HomeController : Controller
    {
        private QueryService _queryService;

        public HomeController(QueryService queryService)
        {
            _queryService = queryService;
        }

        public ActionResult Index(string topic)
        {
            return View(_queryService.GetTopicQueueInfo(topic));
        }
        public ActionResult ConsumeInfo(string group, string topic)
        {
            return View(_queryService.GetTopicConsumeInfo(group, topic));
        }
    }
}