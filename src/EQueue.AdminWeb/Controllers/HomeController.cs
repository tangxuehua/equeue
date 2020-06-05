using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
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

        public async Task<ActionResult> Index()
        {
            var clusterList = await _messageService.GetAllClusters();
            return View(new ClusterListViewModel
            {
                ClusterList = clusterList
            });
        }
        public async Task<ActionResult> BrokerList(string clusterName)
        {
            ViewBag.ClusterName = clusterName;
            var brokerList = await _messageService.GetClusterBrokerStatusInfoList(clusterName);
            return View(new ClusterBrokerListViewModel
            {
                BrokerList = brokerList
            });
        }
        public async Task<ActionResult> QueueInfoList(string clusterName, string topic)
        {
            ViewBag.ClusterName = clusterName;
            var topicQueueInfoList = await _messageService.GetTopicQueueInfoList(clusterName, topic);
            return View(new ClusterTopicQueueListViewModel
            {
                Topic = topic,
                TopicQueueInfoList = topicQueueInfoList
            });
        }
        public async Task<ActionResult> ConsumeInfoList(string clusterName, string group, string topic)
        {
            ViewBag.ClusterName = clusterName;
            var topicConsumeInfoList = await _messageService.GetTopicConsumeInfoList(clusterName, group, topic);
            return View(new ClusterTopicConsumeListViewModel
            {
                Group = group,
                Topic = topic,
                TopicConsumeInfoList = topicConsumeInfoList
            });
        }
        public async Task<ActionResult> ProducerList(string clusterName)
        {
            ViewBag.ClusterName = clusterName;
            var producerList = await _messageService.GetProducerInfoList(clusterName);
            return View(new ClusterProducerListViewModel
            {
                ProducerList = producerList
            });
        }
        public async Task<ActionResult> ConsumerList(string clusterName, string group, string topic)
        {
            ViewBag.ClusterName = clusterName;
            var consumerInfoList = await _messageService.GetConsumerInfoList(clusterName, group, topic);
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
        public async Task<ActionResult> GetMessageById(string clusterName, string searchMessageId)
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
            var message = await _messageService.GetMessageDetail(clusterName, searchMessageId);
            var model = new MessageViewModel { ClusterName = clusterName, SearchMessageId = searchMessageId };
            if (message != null)
            {
                model.ProducerAddress = message.ProducerAddress;
                model.BrokerName = messageIdInfo.BrokerName;
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
        public async Task<ActionResult> GetMessageByQueueOffset(string clusterName, string brokerName, string searchTopic, string searchQueueId, string searchQueueOffset)
        {
            ViewBag.ClusterName = clusterName;
            var model = new MessageViewModel
            {
                SearchTopic = searchTopic,
                SearchQueueId = searchQueueId,
                SearchQueueOffset = searchQueueOffset
            };

            if (string.IsNullOrWhiteSpace(clusterName)
             || string.IsNullOrWhiteSpace(brokerName)
             || string.IsNullOrWhiteSpace(searchTopic)
             || string.IsNullOrWhiteSpace(searchQueueId)
             || string.IsNullOrWhiteSpace(searchQueueOffset))
            {
                return View(model);
            }
            var message = await _messageService.GetMessageDetailByQueueOffset(clusterName, brokerName, searchTopic, int.Parse(searchQueueId), long.Parse(searchQueueOffset));
            if (message != null)
            {
                MessageIdInfo? messageIdInfo = null;
                try
                {
                    messageIdInfo = MessageIdUtil.ParseMessageId(message.MessageId);
                }
                catch {  }

                if (messageIdInfo != null)
                {
                    model.BrokerName = messageIdInfo.Value.BrokerName;
                }
                model.ProducerAddress = message.ProducerAddress;
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
        public async Task<ActionResult> CreateTopic(string clusterName, string topic, int? initialQueueCount)
        {
            ViewBag.ClusterName = clusterName;
            await _messageService.CreateTopic(clusterName, topic, initialQueueCount ?? 4);
            return RedirectToAction("QueueInfoList", new { ClusterName = clusterName, Topic = topic });
        }
        public async Task<ActionResult> DeleteTopic(string clusterName, string topic)
        {
            ViewBag.ClusterName = clusterName;
            await _messageService.DeleteTopic(clusterName, topic);
            return RedirectToAction("QueueInfoList", new { ClusterName = clusterName, Topic = topic });
        }
        public async Task<ActionResult> AddQueue(string clusterName, string topic)
        {
            ViewBag.ClusterName = clusterName;
            await _messageService.AddQueue(clusterName, topic);
            return RedirectToAction("QueueInfoList", new { ClusterName = clusterName, Topic = topic });
        }
        public async Task<ActionResult> DeleteQueue(string clusterName, string topic, int queueId)
        {
            ViewBag.ClusterName = clusterName;
            await _messageService.DeleteQueue(clusterName, topic, queueId);
            return RedirectToAction("QueueInfoList", new { ClusterName = clusterName, Topic = topic });
        }
        public async Task<ActionResult> SetQueueProducerVisible(string clusterName, string topic, int queueId, bool visible)
        {
            ViewBag.ClusterName = clusterName;
            await _messageService.SetQueueProducerVisible(clusterName, topic, queueId, visible);
            return RedirectToAction("QueueInfoList", new { ClusterName = clusterName, Topic = topic });
        }
        public async Task<ActionResult> SetQueueConsumerVisible(string clusterName, string topic, int queueId, bool visible)
        {
            ViewBag.ClusterName = clusterName;
            await _messageService.SetQueueConsumerVisible(clusterName, topic, queueId, visible);
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
        public async Task<ActionResult> SetQueueNextConsumeOffset(string clusterName, string consumerGroup, string topic, int queueId, long nextOffset)
        {
            ViewBag.ClusterName = clusterName;
            await _messageService.SetQueueNextConsumeOffset(clusterName, consumerGroup, topic, queueId, nextOffset);
            return RedirectToAction("ConsumeInfoList", new { ClusterName = clusterName, Group = consumerGroup, Topic = topic });
        }
        public async Task<ActionResult> DeleteConsumerGroup(string clusterName, string consumerGroup)
        {
            ViewBag.ClusterName = clusterName;
            await _messageService.DeleteConsumerGroup(clusterName, consumerGroup);
            return RedirectToAction("ConsumeInfoList", new { ClusterName = clusterName });
        }
    }
}