using System;
using System.Collections.Generic;
using System.Text;
using System.Web.Mvc;
using EQueue.AdminWeb.Models;
using EQueue.Utils;

namespace EQueue.AdminWeb.Controllers
{
    [HandleError]
    public class HomeController : Controller
    {
        private MessageService _messageService;

        public HomeController(MessageService messageService)
        {
            _messageService = messageService;
        }

        public ActionResult Index()
        {
            var clusterList = _messageService.GetAllClusters();
            return View(new ClusterListViewModel
            {
                ClusterList = clusterList
            });
        }
        public ActionResult BrokerList(string clusterName)
        {
            ViewBag.ClusterName = clusterName;
            var brokerList = _messageService.GetClusterBrokerStatusInfoList(clusterName);
            return View(new ClusterBrokerListViewModel
            {
                BrokerList = brokerList
            });
        }
        public ActionResult QueueInfoList(string clusterName, string topic)
        {
            ViewBag.ClusterName = clusterName;
            var topicQueueInfoList = _messageService.GetTopicQueueInfoList(clusterName, topic);
            return View(new ClusterTopicQueueListViewModel
            {
                Topic = topic,
                TopicQueueInfoList = topicQueueInfoList
            });
        }
        public ActionResult ConsumeInfoList(string clusterName, string group, string topic)
        {
            ViewBag.ClusterName = clusterName;
            var topicConsumeInfoList = _messageService.GetTopicConsumeInfoList(clusterName, group, topic);
            return View(new ClusterTopicConsumeListViewModel
            {
                Group = group,
                Topic = topic,
                TopicConsumeInfoList = topicConsumeInfoList
            });
        }
        public ActionResult ProducerList(string clusterName)
        {
            ViewBag.ClusterName = clusterName;
            var producerList = _messageService.GetProducerInfoList(clusterName);
            return View(new ClusterProducerListViewModel
            {
                ProducerList = producerList
            });
        }
        public ActionResult ConsumerList(string clusterName, string group, string topic)
        {
            ViewBag.ClusterName = clusterName;
            var consumerInfoList = _messageService.GetConsumerInfoList(clusterName, group, topic);
            var modelList = new List<ConsumerViewModel>();
            foreach (var consumerInfo in consumerInfoList)
            {
                foreach (var consumer in consumerInfo.ConsumerList)
                {
                    modelList.Add(new ConsumerViewModel
                    {
                        BrokerInfo = consumerInfo.BrokerInfo,
                        ConsumerInfo = consumer
                    });
                }
            }
            modelList.Sort((x, y) =>
            {
                var result = string.Compare(x.ConsumerInfo.ConsumerGroup, y.ConsumerInfo.ConsumerGroup);
                if (result != 0)
                {
                    return result;
                }
                result = string.Compare(x.ConsumerInfo.ConsumerId, y.ConsumerInfo.ConsumerId);
                if (result != 0)
                {
                    return result;
                }
                result = string.Compare(x.BrokerInfo.BrokerName, y.BrokerInfo.BrokerName);
                if (result != 0)
                {
                    return result;
                }
                result = string.Compare(x.ConsumerInfo.Topic, y.ConsumerInfo.Topic);
                if (result != 0)
                {
                    return result;
                }
                if (x.ConsumerInfo.QueueId > y.ConsumerInfo.QueueId)
                {
                    return 1;
                }
                else if (x.ConsumerInfo.QueueId < y.ConsumerInfo.QueueId)
                {
                    return -1;
                }
                return 0;
            });
            return View(new ClusterConsumerListViewModel
            {
                Group = group,
                Topic = topic,
                ConsumerList = modelList
            });
        }
        public ActionResult Message(string clusterName, string searchMessageId)
        {
            ViewBag.ClusterName = clusterName;
            if (string.IsNullOrWhiteSpace(searchMessageId))
            {
                return View(new MessageViewModel());
            }
            MessageIdInfo messageIdInfo;
            try
            {
                messageIdInfo = MessageIdUtil.ParseMessageId(searchMessageId);
            }
            catch (Exception ex)
            {
                throw new Exception("无效的消息ID", ex);
            }
            var message = _messageService.GetMessageDetail(clusterName, searchMessageId);
            var model = new MessageViewModel { ClusterName = clusterName, SearchMessageId = searchMessageId };
            if (message != null)
            {
                model.ProducerAddress = message.ProducerAddress;
                model.BrokerAddress = messageIdInfo.IP.ToString() + ":" + messageIdInfo.Port.ToString();
                model.MessageId = message.MessageId;
                model.Topic = message.Topic;
                model.QueueId = message.QueueId.ToString();
                model.QueueOffset = message.QueueOffset.ToString();
                model.Code = message.Code.ToString();
                model.Payload = Encoding.UTF8.GetString(message.Body);
                model.CreatedTime = message.CreatedTime.ToString("yyyy-MM-dd HH:mm:ss.fff");
                model.StoredTime = message.StoredTime.ToString("yyyy-MM-dd HH:mm:ss.fff");
            }
            return View(model);
        }
        public ActionResult CreateTopic(string clusterName, string topic, int? initialQueueCount)
        {
            ViewBag.ClusterName = clusterName;
            _messageService.CreateTopic(clusterName, topic, initialQueueCount ?? 4);
            return RedirectToAction("QueueInfoList", new { ClusterName = clusterName, Topic = topic });
        }
        public ActionResult DeleteTopic(string clusterName, string topic)
        {
            ViewBag.ClusterName = clusterName;
            _messageService.DeleteTopic(clusterName, topic);
            return RedirectToAction("QueueInfoList", new { ClusterName = clusterName, Topic = topic });
        }
        public ActionResult AddQueue(string clusterName, string topic)
        {
            ViewBag.ClusterName = clusterName;
            _messageService.AddQueue(clusterName, topic);
            return RedirectToAction("QueueInfoList", new { ClusterName = clusterName, Topic = topic });
        }
        public ActionResult DeleteQueue(string clusterName, string topic, int queueId)
        {
            ViewBag.ClusterName = clusterName;
            _messageService.DeleteQueue(clusterName, topic, queueId);
            return RedirectToAction("QueueInfoList", new { ClusterName = clusterName, Topic = topic });
        }
        public ActionResult SetQueueProducerVisible(string clusterName, string topic, int queueId, bool visible)
        {
            ViewBag.ClusterName = clusterName;
            _messageService.SetQueueProducerVisible(clusterName, topic, queueId, visible);
            return RedirectToAction("QueueInfoList", new { ClusterName = clusterName, Topic = topic });
        }
        public ActionResult SetQueueConsumerVisible(string clusterName, string topic, int queueId, bool visible)
        {
            ViewBag.ClusterName = clusterName;
            _messageService.SetQueueConsumerVisible(clusterName, topic, queueId, visible);
            return RedirectToAction("QueueInfoList", new { ClusterName = clusterName, Topic = topic });
        }
        [HttpGet]
        public ActionResult SetQueueNextConsumeOffset(string clusterName, string consumerGroup, string topic, int queueId)
        {
            ViewBag.ClusterName = clusterName;
            var model = new SetQueueNextConsumeOffsetViewModel
            {
                ConsumerGroup = consumerGroup,
                Topic = topic,
                QueueId = queueId
            };
            return View(model);
        }
        [HttpPost]
        public ActionResult SetQueueNextConsumeOffset(string clusterName, string consumerGroup, string topic, int queueId, long nextOffset)
        {
            ViewBag.ClusterName = clusterName;
            _messageService.SetQueueNextConsumeOffset(clusterName, consumerGroup, topic, queueId, nextOffset);
            return RedirectToAction("ConsumeInfoList", new { ClusterName = clusterName, Group = consumerGroup, Topic = topic });
        }
        public ActionResult DeleteConsumerGroup(string clusterName, string consumerGroup)
        {
            ViewBag.ClusterName = clusterName;
            _messageService.DeleteConsumerGroup(clusterName, consumerGroup);
            return RedirectToAction("ConsumeInfoList", new { ClusterName = clusterName });
        }
    }
}