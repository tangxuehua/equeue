using System;

namespace EQueue.Utils
{
    [Serializable]
    public class StringTypeData : TypeData<string>
    {
        public StringTypeData(int typeCode, string data) : base(typeCode, data) { }
    }
}
