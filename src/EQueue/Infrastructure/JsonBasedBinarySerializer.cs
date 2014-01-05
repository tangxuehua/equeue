using System;
using System.Text;

namespace EQueue.Infrastructure
{
    public class JsonBasedBinarySerializer : IBinarySerializer
    {
        private readonly IJsonSerializer _jsonSerializer;

        public JsonBasedBinarySerializer(IJsonSerializer jsonSerializer)
        {
            _jsonSerializer = jsonSerializer;
        }

        public byte[] Serialize(object obj)
        {
            return Encoding.UTF8.GetBytes(_jsonSerializer.Serialize(obj));
        }

        public object Deserialize(byte[] data, Type type)
        {
            return _jsonSerializer.Deserialize(Encoding.UTF8.GetString(data), type);
        }

        public T Deserialize<T>(byte[] data) where T : class
        {
            return _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(data));
        }
    }
}
