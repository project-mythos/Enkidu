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

  type T = Fields.Headers

  //val between_delim = "\\(([^)]+)\\)".r

  def encodeHeader(hdr: (String, String)) = {
    val (key, value) = hdr
    s"${key}:${value}"
  }


  def decodeHeader(s: String): Option[(String, String)] = {

    /*
    val toks_o = between_delim.findFirstIn(s) map { k =>
      k.split(":").toList
    }*/


    val toks = s.split(":").toList
      if (toks.size >= 2) {
        val k :: rest = toks
          val hdr = (k, rest.mkString(":") )
          Some(hdr)
      } else {
        //println("re doesn't work")
        None
      }
    
  }

  def toString(t: T) = {
    val s = t.map {x => encodeHeader(x) } mkString("\n")
    s
  }

  def ofString(s: String) = {
    val list_o = s.split("\n").toList map {x => decodeHeader(x) }

    list_o.foldLeft(empty) {case (acc, o) =>
      if (o.isDefined) acc :+ o.get
      else acc 
    }

  }


  def empty = List[(String, String)]()


  def getMulti(headers: T, key: String): List[String] = {
    headers.filter {case (k, v) => k == key} map {
      case (k, v) => v
    }
  }

  def get(t: T, key: String): Option[String] = {
    val s = getMulti(t, key)

    if (s.size == 0) None
    else Some(s.head)
  }

  def add(t: T, k: String, v: String) = {
    t :+ (k, v)
  }


  def addMulti(t: T, key: String, values: List[String]) = {
    val entries = values.map {v => (key, v)}
    t ++ entries
  }

  def addBatch(t: T, entries: List[(String, String)]) = {
    t ++ entries
  }


  def remove(t: T, key: String) = {
    t.filter {case (k, v) => key != k}
  }


}
