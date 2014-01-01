using System.Collections.Concurrent;

namespace EQueue.Infrastructure.Extensions
{
    public static class ConcurrentDictionaryExtensions
    {
        public static void Remove<TKey, TValue>(this ConcurrentDictionary<TKey, TValue> dict, TKey key)
        {
            TValue value;
            dict.TryRemove(key, out value);
        }
    }
}
