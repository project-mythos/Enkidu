package Enkidu.Mux

import java.nio.charset.StandardCharsets.UTF_8
import io.netty.handler.codec.{
  ByteToMessageDecoder,
  MessageToByteEncoder
}


import scala.util.matching.Regex
import io.netty.buffer.{Unpooled, ByteBuf, ByteBufUtil}



trait MSG[T] {

  def headers(t: T): Fields.Headers
  def body(t: T): Array[Byte]

  def withHeaders(t: T, hdrs: Fields.Headers): T
  def withBody(t: T, body: Array[Byte]): T
}


case class TMSG(
  path: Fields.Path,
  headers: Fields.Headers,
  payload: Array[Byte]
) 

object TMSG extends MSG[TMSG] {

  def apply(path: Fields.Path, payload: Array[Byte]): TMSG = {
    TMSG(path, Headers.empty, payload)
  }

  def apply(path: Fields.Path): TMSG = {
    TMSG(path, Headers.empty, Array[Byte]() )
  }

  def body(t: TMSG) = t.payload
  def headers(t: TMSG) = t.headers

  def withHeaders(t: TMSG, headers: Fields.Headers) = {
    t.copy(headers=headers)
  }

  def withBody(t: TMSG, body: Array[Byte]) = t.copy(payload=body)

}


case class RMSG(headers: Fields.Headers, payload: Array[Byte])


object RMSG extends MSG[RMSG]  {

  def apply(payload: Array[Byte]): RMSG  = {
    RMSG(Headers.empty, payload )
  }

  def empty(): RMSG ={
    RMSG( Array[Byte]()  )
  }

  def body(t: RMSG) = t.payload
  def headers(t: RMSG) = t.headers

  def withHeaders(t: RMSG, headers: Fields.Headers) = {
    t.copy(headers=headers)
  }

  def withBody(t: RMSG, body: Array[Byte]) = t.copy(payload=body)

}




object Helpers {
  def byteBufToArray(buf: ByteBuf): Array[Byte] = {
    if ( buf.hasArray() ) return buf.array()

    val array = new Array[Byte](buf.readableBytes)
    buf.readBytes(array)
    return array
  }

  def empty = List[(String, String)]()


}


