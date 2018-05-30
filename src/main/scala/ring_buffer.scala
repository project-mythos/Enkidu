import com.twitter.util.{Future, Time}
import java.util.concurrent.atomic.{AtomicLong, AtomicBoolean }
import scala.annotation.tailrec
import scala.reflect.ClassTag


class RingBuffer[T : ClassTag](capacity: Int) {
  assert(capacity > 0)

  private[this] val nextRead, nextWrite = new AtomicLong(0)
  private[this] val publishedWrite = new AtomicLong(-1)
  private[this] val ring = new Array[T](capacity)

  private[this] def publish(which: Long) {
    while (publishedWrite.get != which - 1) {}
    val ok = publishedWrite.compareAndSet(which - 1, which)
    assert(ok)
  }

  /**
    * Try to get an item out of the ConcurrentRingBuffer. Returns no item only
    * when the buffer is empty.
    */
  @tailrec
  final def tryGet(): Option[T] = {
    val w = publishedWrite.get
    val r = nextRead.get

    // Note that the race here is intentional: even if another thread
    // adds an item between getting the r/w cursors and testing them,
    // we have still maintain visibility.
    if (w < r)
      return None

    val el = ring((r%capacity).toInt)
    if (nextRead.compareAndSet(r, r+1))
      Some(el)
    else
      tryGet()
  }

  /**
    * Returns the next element without changing the read position.
    *
    * @return the next element or None if buffer is empty
    */
  final def tryPeek: Option[T] = {
    val w = publishedWrite.get
    val r = nextRead.get

    if (w < r) None
    else Some(ring((r%capacity).toInt))
  }

  /**
    * Attempt to put an item into the buffer. If it is full, the
    * operation fails.
    */
  @tailrec
  final def tryPut(el: T): Boolean = {
    val w = nextWrite.get
    val r = nextRead.get

    if (w - r >= capacity)
      return false

    if (!nextWrite.compareAndSet(w, w+1)) tryPut(el) else {
      ring((w%capacity).toInt) = el
      publish(w)
      true
    }
  }

  /**
    * Current size of the buffer.
    *
    * The returned value is only quiescently consistent; treat it
    * as a fast approximation.
    */
  final def size: Int = (nextWrite.get - nextRead.get).toInt
}
