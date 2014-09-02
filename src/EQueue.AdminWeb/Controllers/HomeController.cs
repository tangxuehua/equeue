using System.Web.Mvc;
using EQueue.AdminWeb.Models;

namespace EQueue.AdminWeb.Controllers
{
    public class HomeController : Controller
    {
        private MessageService _messageService;

        public HomeController(MessageService messageService)
        {
            _messageService = messageService;
        }

        public ActionResult Index(string topic)
        {
            var topicQueueInfos = _messageService.GetTopicQueueInfo(topic);
            return View(new TopicQueueViewModel
            {
                Topic = topic,
                TopicQueueInfos = topicQueueInfos
            });
        }
        public ActionResult ConsumerInfo(string group, string topic)
        {
            var consumerInfos = _messageService.GetConsumerInfo(group, topic);
            return View(new ConsumerViewModel
            {
                Group = group,
                Topic = topic,
                ConsumerInfos = consumerInfos
            });
        }
        public ActionResult TopicConsumeInfo(string group, string topic)
        {
            var topicConsumeInfos = _messageService.GetTopicConsumeInfo(group, topic);
            return View(new TopicConsumeViewModel
            {
                Group = group,
                Topic = topic,
                TopicConsumeInfos = topicConsumeInfos
            });
        }
        public ActionResult AddQueue(string topic)
        {
            _messageService.AddQueue(topic);
            return RedirectToAction("Index");
        }
        public ActionResult RemoveQueue(string topic, int queueId)
        {
            _messageService.RemoveQueue(topic, queueId);
            return RedirectToAction("Index");
        }
        public ActionResult EnableQueue(string topic, int queueId)
        {
            _messageService.EnableQueue(topic, queueId);
            return RedirectToAction("Index");
        }
        public ActionResult DisableQueue(string topic, int queueId)
        {
            _messageService.DisableQueue(topic, queueId);
            return RedirectToAction("Index");
        }
    }
}