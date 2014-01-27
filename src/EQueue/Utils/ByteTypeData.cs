using System;

namespace EQueue.Utils
{
    [Serializable]
    public class ByteTypeData : TypeData<byte[]>
    {
        public ByteTypeData(int typeCode, byte[] data) : base(typeCode, data) { }
    }
}
