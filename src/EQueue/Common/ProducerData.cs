using System;

namespace EQueue.Common
{
    [Serializable]
    public class ProducerData
    {
        public string GroupName { get; private set; }

        public ProducerData(string groupName)
        {
            GroupName = groupName;
        }

        public override string ToString()
        {
            return GroupName;
        }
    }
}
