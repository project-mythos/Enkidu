package Enkidu.Mux

import java.nio.charset.StandardCharsets.UTF_8
import io.netty.handler.codec.{
  ByteToMessageDecoder,
  MessageToByteEncoder
}


import scala.util.matching.Regex
import io.netty.buffer.{Unpooled, ByteBuf, ByteBufUtil}

object Fields {
  type Path = List[String]
  type Headers = List[(String, String)]
}


trait StringField[T] {
  def toString(t: T): String
  def ofString(t: String): T

  //this can be a good thing to change when trying to shave off some latency
  def toByteBuf(p: T) = {
    val s = toString(p)

    val bytes = s.getBytes("UTF-8")
    (bytes.length, Unpooled.wrappedBuffer(bytes) )
  }



  def ofByteBuf(buf: ByteBuf) = {
    val s =  buf.toString(UTF_8)
    ofString(s)
  }

}



object Path extends StringField[Fields.Path]  {

  def toString(p: Fields.Path): String = p.mkString("/")
  def ofString(s: String): Fields.Path = s.split("/").toList
}




object Headers extends StringField[Fields.Headers] {

  val between_delim = "\\{.*?\\}".r
  val pairs = "\\[.*?\\]".r


  def headerToString(hdr: (String, String)) = {
    val (key, value) = hdr
    s"${key}:${value}"
  }

  def toString(t: List[(String, String)]) = {
    val t1 = t.map {x => headerToString(x) }
    "{" + t1.mkString(", ") + "}"
  }


  def ofString(s: String): Fields.Headers = {
    val h1 = between_delim.findFirstIn(s)
    val hdrs = h1.map {x => pairs.findAllIn(x).toList }



    val getHdr = {l: List[String] =>

      l.map { x =>
        val toks = x.split(":")
        (toks(0), toks(1) )
      }

    }

    val o = hdrs.map { x => getHdr(x)}

    o match {
      case Some(x) => x
      case _ => List()
    }

  }



  def empty = List[(String, String)]()

}

case class TMSG(
  path: Fields.Path,
  headers: Fields.Headers,
  payload: Array[Byte]
)

object TMSG {

  def apply(path: Fields.Path, payload: Array[Byte]): TMSG = {
    TMSG(path, Headers.empty, payload)
  }

}


case class RMSG(headers: Fields.Headers, payload: Array[Byte])

object RMSG  {

  def apply(payload: Array[Byte]): RMSG  = {
    RMSG( Headers.empty, payload)
  }

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


