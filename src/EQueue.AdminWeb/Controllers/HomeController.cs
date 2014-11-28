using System.Web.Mvc;
using ECommon.Serializing;
using EQueue.AdminWeb.Models;

namespace EQueue.AdminWeb.Controllers
{
    public class HomeController : Controller
    {
        private MessageService _messageService;
        private IBinarySerializer _binarySerializer;

        public HomeController(MessageService messageService, IBinarySerializer binarySerializer)
        {
            _messageService = messageService;
            _binarySerializer = binarySerializer;
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
        public ActionResult Messages(string topic, int? queueId, int? code)
        {
            var messages = _messageService.QueryMessages(topic, queueId, code);
            return View(new MessagesViewModel
            {
                Topic = topic,
                QueueId = queueId,
                Code = code,
                Messages = messages
            });
        }
        public ActionResult Message(long messageOffset)
        {
            var message = _messageService.GetMessageDetail(messageOffset);
            return View(new MessageViewModel
            {
                MessageOffset = messageOffset,
                Message = message,
                //MessageContent = _binarySerializer.Deserialize  TODO
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
        public ActionResult RemoveQueueOffsetInfo(string consumerGroup, string topic, int queueId)
        {
            _messageService.RemoveQueueOffsetInfo(consumerGroup, topic, queueId);
            return RedirectToAction("Index");
        }
    }
}