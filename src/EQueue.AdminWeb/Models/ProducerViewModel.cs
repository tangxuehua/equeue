using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.AdminWeb.Models
{
    public class ProducerViewModel
    {
        public IEnumerable<string> ProducerIds { get; set; }
    }
}