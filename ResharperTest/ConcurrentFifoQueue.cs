namespace Log4Net.Async
{
    public sealed class ConcurrentFifoQueue<T> : IQueue<T>
    {
        private readonly object m_LockObject = new object();
        private readonly T[] m_Buffer;
        private readonly int m_Size;
        private int m_ReadIndex;
        private int m_WriteIndex;

        public int Size { get { return m_Size-1; } }

        public ConcurrentFifoQueue(int size)
        {
            m_Size = size+1;
            m_Buffer = new T[size+1];
        }

        /// <summary>
        /// Returns true if item has been enqueued and false otherwise.
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public bool TryEnqueue(T item)
        {
            lock (m_LockObject)
            {
                if (IsFull())
                    return false;
                m_Buffer[m_WriteIndex] = item;
                m_WriteIndex = NextIndex(m_WriteIndex);
            }
            return true;
        }

        /// <summary>
        /// Returns true if item has been dequeued and false otherwise.
        /// </summary>
        /// <param name="ret"></param>
        /// <returns></returns>
        public bool TryDequeue(out T ret)
        {
            lock (m_LockObject)
            {
                if (!IsEmpty())
                {
                    ret = m_Buffer[m_ReadIndex];
                    m_Buffer[m_ReadIndex] = default(T);
                    m_ReadIndex = NextIndex(m_ReadIndex);
                    return true;
                }
            }
            ret = default(T);
            return false;
        }

        private int NextIndex(int index)
        {
            return (index + 1) % m_Size;
        }

        private bool IsEmpty()
        {
            return m_ReadIndex == m_WriteIndex;
        }

        private bool IsFull()
        {
            return NextIndex(m_WriteIndex) == m_ReadIndex;
        }
    }
}