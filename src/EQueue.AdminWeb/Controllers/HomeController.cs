using System.Web.Mvc;
using EQueue.AdminWeb.Models;

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
            var topicQueueInfos = _queryService.GetTopicQueueInfo(topic);
            return View(new TopicQueueViewModel
            {
                Topic = topic,
                TopicQueueInfos = topicQueueInfos
            });
        }
        public ActionResult ConsumeInfo(string group, string topic)
        {
            var topicConsumeInfos = _queryService.GetTopicConsumeInfo(group, topic);
            return View(new TopicConsumeViewModel
            {
                Group = group,
                Topic = topic,
                TopicConsumeInfos = topicConsumeInfos
            });
        }
    }
}