namespace Log4Net.Async
{
    public interface IQueue<T>
    {
        /// <summary>
        /// Returns true if item has been enqueued and false otherwise.
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        bool TryEnqueue(T item);

        /// <summary>
        /// Returns true if item has been dequeued and false otherwise.
        /// </summary>
        /// <param name="ret"></param>
        /// <returns></returns>
        bool TryDequeue(out T ret);
    }
}
