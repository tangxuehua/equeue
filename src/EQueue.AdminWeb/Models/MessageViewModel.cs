namespace EQueue.AdminWeb.Models
{
    public class MessageViewModel
    {
        public string ClusterName { get; set; }
        public string ProducerAddress { get; set; }
        public string BrokerAddress { get; set; }
        public string SearchMessageId { get; set; }
        public string MessageId { get; set; }
        public string QueueId { get; set; }
        public string QueueOffset { get; set; }
        public string Topic { get; set; }
        public string Code { get; set; }
        public string CreatedTime { get; set; }
        public string StoredTime { get; set; }
        public string Payload { get; set; }
    }
}