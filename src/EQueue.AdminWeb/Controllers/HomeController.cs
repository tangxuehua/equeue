using System.Text;
using System.Web.Mvc;
using EQueue.AdminWeb.Controls;
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
        public ActionResult Messages(string topic, int? queueId, int? code, string routingKey, int? page)
        {
            var currentPage = page ?? 1;
            var size = 20;
            if (currentPage <= 0) currentPage = 1;

            var result = _messageService.QueryMessages(topic, queueId, code, routingKey, currentPage, size);

            ViewBag.Topic = topic;
            ViewBag.QueueId = queueId;
            ViewBag.Code = code;
            ViewBag.RoutingKey = routingKey;
            ViewBag.Pager = Pager.Items(result.Total).PerPage(size).Move(currentPage).Segment(5).Center();

            return View(new MessagesViewModel
            {
                Topic = topic,
                QueueId = queueId,
                Code = code,
                RoutingKey = routingKey,
                Total = result.Total,
                Messages = result.Messages
            });
        }
        public ActionResult Message(long? searchMessageOffset)
        {
            if (searchMessageOffset == null)
            {
                return View(new MessageViewModel());
            }
            var message = _messageService.GetMessageDetail(searchMessageOffset.Value);
            var model = new MessageViewModel { SearchMessageOffset = searchMessageOffset.Value.ToString() };
            if (message != null)
            {
                model.MessageOffset = message.MessageOffset.ToString();
                model.QueueId = message.QueueId.ToString();
                model.QueueOffset = message.QueueOffset.ToString();
                model.RoutingKey = message.RoutingKey;
                model.Code = message.Code.ToString();
                model.Content = Encoding.UTF8.GetString(message.Body);
                model.Topic = message.Topic;
                model.StoredTime = message.StoredTime.ToString();
            }
            return View(model);
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
        public ActionResult RemoveQueueOffsetInfo(string consumerGroup, string topic, int queueId)
        {
            _messageService.RemoveQueueOffsetInfo(consumerGroup, topic, queueId);
            return RedirectToAction("Index");
        }
    }
}