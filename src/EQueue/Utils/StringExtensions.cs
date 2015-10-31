using System;

namespace EQueue.Utils
{
    public static class StringExtensions
    {
        public static int GetHashcode2(this string s)
        {
            if (string.IsNullOrEmpty(s)) return 0;

            unchecked
            {
                int hash = 23;
                foreach (char c in s)
                {
                    hash = (hash << 5) - hash + c;
                }
                if (hash < 0)
                {
                    hash = Math.Abs(hash);
                }
                return hash;
            }
        }
    }
}
