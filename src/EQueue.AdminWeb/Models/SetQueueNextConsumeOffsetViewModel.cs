namespace EQueue.AdminWeb.Models
{
    public class SetQueueNextConsumeOffsetViewModel
    {
        public string ConsumerGroup { get; set; }
        public string Topic { get; set; }
        public int QueueId { get; set; }
        public long NextOffset { get; set; }
    }
}