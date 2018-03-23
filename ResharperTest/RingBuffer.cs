using System;

namespace Log4Net.Async
{
    public sealed class RingBuffer<T> : IQueue<T>
    {
        private readonly object m_LockObject = new object();
        private readonly T[] m_Buffer;
        private readonly int m_Size;
        private int m_ReadIndex;
        private int m_WriteIndex;
        private bool m_BufferFull;

        public int Size { get { return m_Size; } }

        public event Action<object, EventArgs> BufferOverflow;

        public RingBuffer(int size)
        {
            m_Size = size;
            m_Buffer = new T[size];
        }

        public bool TryEnqueue(T item)
        {
            var bufferWasFull = false;
            lock (m_LockObject)
            {
                m_Buffer[m_WriteIndex] = item;
                m_WriteIndex = (++m_WriteIndex) % m_Size;
                if (m_BufferFull)
                {
                    bufferWasFull = true;
                    m_ReadIndex = m_WriteIndex;
                }
                else if (m_WriteIndex == m_ReadIndex)
                {
                    m_BufferFull = true;
                }
            }

            if (bufferWasFull)
            {
                if (BufferOverflow != null)
                {
                    BufferOverflow(this, EventArgs.Empty);
                }
            }
            return !bufferWasFull;
        }

        public bool TryDequeue(out T ret)
        {
            if (m_ReadIndex == m_WriteIndex && !m_BufferFull)
            {
                ret = default(T);
                return false;
            }
            lock (m_LockObject)
            {
                if (m_ReadIndex == m_WriteIndex && !m_BufferFull)
                {
                    ret = default(T);
                    return false;
                }

                ret = m_Buffer[m_ReadIndex];
                m_Buffer[m_ReadIndex] = default(T);
                m_ReadIndex = (++m_ReadIndex) % m_Size;
                m_BufferFull = false;
                return true;
            }
        }
    }
}