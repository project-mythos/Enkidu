package Enkidu.IO

import io.netty.buffer._




import com.twitter.io.{Buf, ByteReader}
import com.twitter.io.ByteReader.UnderflowException
import java.lang.{Double => JDouble, Float => JFloat}

import io.netty.buffer.ByteBuf
import io.netty.util.ByteProcessor
import java.nio.charset.Charset

import java.lang.{Double => JDouble, Float => JFloat}

object ByteBufConversion {


  import com.twitter.io

  // Assuming that bb.hasArray.

  def heapToBuf(bb: ByteBuf): Buf.ByteArray = {
    val begin = bb.arrayOffset + bb.readerIndex
    val end = begin + bb.readableBytes
    new Buf.ByteArray(bb.array, begin, end)
  }

  /** Make a copied `Buf.ByteArray` representation of the provided `ByteBuf`. */
  def copyByteBufToBuf(bb: ByteBuf): Buf.ByteArray = {
    val array = new Array[Byte](bb.readableBytes)
    bb.readBytes(array)
    new io.Buf.ByteArray(array, 0, array.length)
  }

  /**
   * A read-only and potentially non-copying `ByteBuf` representation of a [[Buf]].
   */
  def bufAsByteBuf(buf: Buf): ByteBuf = {
    val bb = buf match {
      case _ if buf.isEmpty =>
        Unpooled.EMPTY_BUFFER
      case Buf.ByteArray.Owned(bytes, begin, end) =>
        Unpooled.wrappedBuffer(bytes, begin, end - begin)
      case _ =>
        Unpooled.wrappedBuffer(Buf.ByteBuffer.Owned.extract(buf))
    }

    bb.asReadOnly
  }

  /**
   * Construct a [[Buf]] from a `ByteBuf`, releasing it.
   *
   * @note if the given is backed by a heap array, it will be coerced into `Buf.ByteArray`
   *       and then released. This basically means it's only safe to use this smart constructor
   *       with heap buffers which are unpooled, and non-heap buffers.
   */
  def byteBufAsBuf(buf: ByteBuf): Buf =
    if (buf.readableBytes == 0) Buf.Empty
    else try {
      if (buf.hasArray) heapToBuf(buf)
      else copyByteBufToBuf(buf)
    } finally buf.release()
}





abstract class AbstractByteBufByteReader(bb: ByteBuf) extends ByteReader {

  private[this] var closed: Boolean = false

  final def remaining: Int = bb.readableBytes()

  final def remainingUntil(byte: Byte): Int = bb.bytesBefore(byte)

  def process(processor: Buf.Processor): Int =
    process(0, bb.readableBytes(), processor)

  def process(from: Int, until: Int, processor: Buf.Processor): Int =
    AbstractByteBufByteReader.process(from, until, processor, bb)

  final def readString(numBytes: Int, charset: Charset): String = {
    checkRemaining(numBytes)
    val result = bb.toString(bb.readerIndex, numBytes, charset)
    bb.readerIndex(bb.readerIndex + numBytes)
    result
  }

  final def readByte(): Byte = {
    checkRemaining(1)
    bb.readByte()
  }

  final def readUnsignedByte(): Short = {
    checkRemaining(1)
    bb.readUnsignedByte()
  }

  final def readShortBE(): Short = {
    checkRemaining(2)
    bb.readShort()
  }

  final def readShortLE(): Short = {
    checkRemaining(2)
    bb.readShortLE()
  }

  final def readUnsignedShortBE(): Int = {
    checkRemaining(2)
    bb.readUnsignedShort()
  }

  final def readUnsignedShortLE(): Int = {
    checkRemaining(2)
    bb.readUnsignedShortLE()
  }

  final def readMediumBE(): Int = {
    checkRemaining(3)
    bb.readMedium()
  }

  final def readMediumLE(): Int = {
    checkRemaining(3)
    bb.readMediumLE()
  }

  final def readUnsignedMediumBE(): Int = {
    checkRemaining(3)
    bb.readUnsignedMedium()
  }

  final def readUnsignedMediumLE(): Int = {
    checkRemaining(3)
    bb.readUnsignedMediumLE()
  }

  final def readIntBE(): Int = {
    checkRemaining(4)
    bb.readInt()
  }

  final def readIntLE(): Int = {
    checkRemaining(4)
    bb.readIntLE()
  }

  final def readUnsignedIntBE(): Long = {
    checkRemaining(4)
    bb.readUnsignedInt()
  }

  final def readUnsignedIntLE(): Long = {
    checkRemaining(4)
    bb.readUnsignedIntLE()
  }

  final def readLongBE(): Long = {
    checkRemaining(8)
    bb.readLong()
  }

  final def readLongLE(): Long = {
    checkRemaining(8)
    bb.readLongLE()
  }

  final def readUnsignedLongBE(): BigInt = {
    checkRemaining(8)

    // 9 (8+1) so sign bit is always positive
    val bytes: Array[Byte] = new Array[Byte](9)
    bytes(0) = 0
    bytes(1) = bb.readByte()
    bytes(2) = bb.readByte()
    bytes(3) = bb.readByte()
    bytes(4) = bb.readByte()
    bytes(5) = bb.readByte()
    bytes(6) = bb.readByte()
    bytes(7) = bb.readByte()
    bytes(8) = bb.readByte()

    BigInt(bytes)
  }

  final def readUnsignedLongLE(): BigInt = {
    checkRemaining(8)

    // 9 (8+1) so sign bit is always positive
    val bytes: Array[Byte] = new Array[Byte](9)
    bytes(8) = bb.readByte()
    bytes(7) = bb.readByte()
    bytes(6) = bb.readByte()
    bytes(5) = bb.readByte()
    bytes(4) = bb.readByte()
    bytes(3) = bb.readByte()
    bytes(2) = bb.readByte()
    bytes(1) = bb.readByte()
    bytes(0) = 0

    BigInt(bytes)
  }

  final def readFloatBE(): Float = {
    JFloat.intBitsToFloat(readIntBE())
  }

  final def readFloatLE(): Float = {
    JFloat.intBitsToFloat(readIntLE())
  }

  final def readDoubleBE(): Double = {
    JDouble.longBitsToDouble(readLongBE())
  }

  final def readDoubleLE(): Double = {
    JDouble.longBitsToDouble(readLongLE())
  }

  final def skip(n: Int): Unit = {
    if (n < 0) {
      throw new IllegalArgumentException(s"'n' must be non-negative: $n")
    }
    checkRemaining(n)
    bb.skipBytes(n)
  }

  final def readAll(): Buf = readBytes(remaining)

  final def close(): Unit = {
    if (!closed) {
      closed = true
      bb.release()
    }
  }

  final protected def checkRemaining(needed: Int): Unit =
    if (remaining < needed) {
      throw new UnderflowException(
        s"tried to read $needed byte(s) when remaining bytes was $remaining"
      )
    }
}

private object AbstractByteBufByteReader {
  /**
   * Process `bb` 1-byte at a time using the given
   * [[Buf.Processor]], starting at index `from` of `bb` until
   * index `until`. Processing will halt if the processor
   * returns `false` or after processing the final byte.
   *
   * @return -1 if the processor processed all bytes or
   *         the last processed index if the processor returns
   *         `false`.
   *         Will return -1 if `from` is greater than or equal to
   *         `until` or `length` of the underlying buffer.
   *         Will return -1 if `until` is greater than or equal to
   *         `length` of the underlying buffer.
   *
   * @param from the starting index, inclusive. Must be non-negative.
   *
   * @param until the ending index, exclusive. Must be non-negative.
   */
  private def process(from: Int, until: Int, processor: Buf.Processor, bb: ByteBuf): Int = {
    Buf.checkSliceArgs(from, until)

    val length = bb.readableBytes()

    // check if chunk to process is empty
    if (until <= from || from >= length) -1
    else {
      val byteProcessor = new ByteProcessor {
        def process(value: Byte): Boolean = processor(value)
      }
      val readerIndex = bb.readerIndex()
      val off = readerIndex + from
      val len = math.min(length - from, until - from)
      val index = bb.forEachByte(off, len, byteProcessor)
      if (index == -1) -1
      else index - readerIndex
    }
  }
}


class CopyingByteBufByteReader(bb: ByteBuf)
    extends AbstractByteBufByteReader(bb) {

  /**
   * Returns a new buffer representing a slice of this buffer, delimited
   * by the indices `[cursor, remaining)`. Out of bounds indices are truncated.
   * Negative indices are not accepted.
   *
   * @note the returned `Buf` will be an explicit copy form the underlying `ByteBuf` to a
   *       `ByteArray` backed by only the bytes only usable the bytes.
   */
  def readBytes(n: Int): Buf = {
    if (n < 0) throw new IllegalArgumentException(s"'n' must be non-negative: $n")
    else if (n == 0) Buf.Empty
    else {
      val toRead = math.min(n, remaining)
      val arr = new Array[Byte](toRead)
      bb.readBytes(arr)
      Buf.ByteArray.Owned(arr)
    }
  }
}

