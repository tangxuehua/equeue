using System.Text;
using System.Web.Mvc;
using EQueue.AdminWeb.Models;

namespace EQueue.AdminWeb.Controllers
{
    [HandleError]
    public class BrokerController : Controller
    {
        private MessageService _messageService;

        public BrokerController(MessageService messageService)
        {
            _messageService = messageService;
        }

        public ActionResult Index(string clusterName, string brokerName)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            var result = _messageService.QueryBrokerStatisticInfo(clusterName, brokerName);
            return View(new BrokerStatisticInfoViewModel
            {
                TopicCount = result.TopicCount,
                QueueCount = result.QueueCount,
                ConsumerGroupCount = result.ConsumerGroupCount,
                ProducerCount = result.ProducerCount,
                ConsumerCount = result.ConsumerCount,
                TotalUnConsumedMessageCount = result.TotalUnConsumedMessageCount,
                MessageChunkCount = result.MessageChunkCount,
                MessageMaxChunkNum = result.MessageMaxChunkNum,
                MessageMinChunkNum = result.MessageMinChunkNum
            });
        }
        public ActionResult CreateTopic(string clusterName, string brokerName, string topic, int? initialQueueCount)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            _messageService.CreateTopic(clusterName, brokerName, topic, initialQueueCount ?? 4);
            return RedirectToAction("QueueInfo", new { ClusterName = clusterName, BrokerName = brokerName });
        }
        public ActionResult DeleteTopic(string clusterName, string brokerName, string topic)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            _messageService.DeleteTopic(clusterName, brokerName, topic);
            return RedirectToAction("QueueInfo", new { ClusterName = clusterName, BrokerName = brokerName });
        }
        public ActionResult QueueInfo(string clusterName, string brokerName, string topic)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            var topicQueueInfos = _messageService.GetTopicQueueInfo(clusterName, brokerName, topic);
            return View(new TopicQueueViewModel
            {
                Topic = topic,
                TopicQueueInfos = topicQueueInfos
            });
        }
        public ActionResult ConsumeInfo(string clusterName, string brokerName, string group, string topic)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            var topicConsumeInfoList = _messageService.GetTopicConsumeInfo(clusterName, brokerName, group, topic);
            return View(new TopicConsumeInfoViewModel
            {
                Group = group,
                Topic = topic,
                TopicConsumeInfoList = topicConsumeInfoList
            });
        }
        public ActionResult ProducerInfo(string clusterName, string brokerName, string group, string topic)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            var producerIds = _messageService.GetProducerInfo(clusterName, brokerName);
            return View(new ProducerViewModel
            {
                ProducerIds = producerIds
            });
        }
        public ActionResult ConsumerInfo(string clusterName, string brokerName, string group, string topic)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            var consumerInfos = _messageService.GetConsumerInfo(clusterName, brokerName, group, topic);
            return View(new ConsumerViewModel
            {
                Group = group,
                Topic = topic,
                ConsumerInfos = consumerInfos
            });
        }
        public ActionResult AddQueue(string clusterName, string brokerName, string topic)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            _messageService.AddQueue(clusterName, brokerName, topic);
            return RedirectToAction("QueueInfo", new { ClusterName = clusterName, BrokerName = brokerName });
        }
        public ActionResult DeleteQueue(string clusterName, string brokerName, string topic, int queueId)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            _messageService.DeleteQueue(clusterName, brokerName, topic, queueId);
            return RedirectToAction("QueueInfo", new { ClusterName = clusterName, BrokerName = brokerName });
        }
        public ActionResult SetQueueProducerVisible(string clusterName, string brokerName, string topic, int queueId, bool visible)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            _messageService.SetQueueProducerVisible(clusterName, brokerName, topic, queueId, visible);
            return RedirectToAction("QueueInfo", new { ClusterName = clusterName, BrokerName = brokerName });
        }
        public ActionResult SetQueueConsumerVisible(string clusterName, string brokerName, string topic, int queueId, bool visible)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            _messageService.SetQueueConsumerVisible(clusterName, brokerName, topic, queueId, visible);
            return RedirectToAction("QueueInfo", new { ClusterName = clusterName, BrokerName = brokerName });
        }
        [HttpGet]
        public ActionResult SetQueueNextConsumeOffset(string clusterName, string brokerName, string consumerGroup, string topic, int queueId)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            var model = new SetQueueNextConsumeOffsetViewModel
            {
                ConsumerGroup = consumerGroup,
                Topic = topic,
                QueueId = queueId
            };
            return View(model);
        }
        [HttpPost]
        public ActionResult SetQueueNextConsumeOffset(string clusterName, string brokerName, string consumerGroup, string topic, int queueId, long nextOffset)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            _messageService.SetQueueNextConsumeOffset(clusterName, brokerName, consumerGroup, topic, queueId, nextOffset);
            return RedirectToAction("ConsumerInfo", new { ClusterName = clusterName, BrokerName = brokerName });
        }
        public ActionResult DeleteConsumerGroup(string clusterName, string brokerName, string consumerGroup)
        {
            ViewBag.ClusterName = clusterName;
            ViewBag.BrokerName = brokerName;
            _messageService.DeleteConsumerGroup(clusterName, brokerName, consumerGroup);
            return RedirectToAction("ConsumerInfo", new { ClusterName = clusterName, BrokerName = brokerName });
        }
    }
}