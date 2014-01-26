using System;

namespace EQueue.Utils
{
    public class ByteTypeDataUtils
    {
        public static byte[] Encode(ByteTypeData byteTypeData)
        {
            var typeCodeBytes = BitConverter.GetBytes(byteTypeData.TypeCode);
            var data = new byte[typeCodeBytes.Length + byteTypeData.Data.Length];

            typeCodeBytes.CopyTo(data, 0);
            byteTypeData.Data.CopyTo(data, typeCodeBytes.Length);

            return data;
        }
        public static ByteTypeData Decode(byte[] buffer)
        {
            var typeCodeBytes = new byte[4];
            var dataBytes = new byte[buffer.Length - 4];
            Array.Copy(buffer, 0, typeCodeBytes, 0, 4);
            Array.Copy(buffer, 4, dataBytes, 0, dataBytes.Length);

            var typeCode = BitConverter.ToInt32(typeCodeBytes, 0);

            return new ByteTypeData(typeCode, dataBytes);
        }
    }
}
