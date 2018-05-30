package Enkidu.Mux

import io.netty.handler.codec.{
  ByteToMessageDecoder,
  MessageToByteEncoder
}


import io.netty.channel.{ChannelHandlerContext, ChannelPipeline}
import io.netty.buffer.{Unpooled, ByteBuf, ByteBufUtil}


class TMSGDecoder extends ByteToMessageDecoder {

  def decodeField(in: ByteBuf): Option[ByteBuf] = {
    val length = in.readInt()
    if (in.readableBytes() < length) return None
    Some { in.readSlice(length) }  
  }


  override def decode(ctx: ChannelHandlerContext, buf: ByteBuf, out: java.util.List[Object]) = {
    val pathF = decodeField(buf) map {x => Path.ofByteBuf(x) }
    val hdro = decodeField(buf) map {x => Headers.ofByteBuf(x) } 
    val plF = decodeField(buf) map {x => Helpers.byteBufToArray(x)} 


    (pathF, hdro, plF) match {
      case ( Some(path), Some(headers),  Some(payload) ) =>
        val msg = TMSG(path, headers, payload) 
        out.add(msg)

      case _ => buf.resetReaderIndex() 
    }

  }


}




class TMSGEncoder extends MessageToByteEncoder[TMSG]  {

  def encode(ctx: ChannelHandlerContext, msg: TMSG, out: ByteBuf) = {

    val (plen, path) = Path.toByteBuf(msg.path)
    out.writeInt(plen)
    out.writeBytes(path)


    val (clen, ctxb) = Headers.toByteBuf(msg.headers)
    out.writeInt(clen)
    out.writeBytes(ctxb)


    out.writeInt(msg.payload.length)
    out.writeBytes(msg.payload)

  }
}




class RMSGDecoder extends ByteToMessageDecoder {


  def decodeField(in: ByteBuf): Option[ByteBuf] = {
    val length = in.readInt()
    if (in.readableBytes() < length) return None
    Some { in.readSlice(length) }
  }


  def decode(ctx: ChannelHandlerContext, buf: ByteBuf, out: java.util.List[Object]) = {
    val hdro = decodeField(buf) map {x => Headers.ofByteBuf(x) }
    val plF = decodeField(buf) map {x => Helpers.byteBufToArray(x) }

    (hdro, plF) match {
      case (Some(headers), Some(payload) ) =>
        val msg = RMSG(headers, payload)
        out.add(msg)

      case _ => buf.resetReaderIndex() 
    }

  }



}



class RMSGEncoder extends MessageToByteEncoder[RMSG] {



  def encode(ctx: ChannelHandlerContext, msg: RMSG, out: ByteBuf) = {
    val (clen, ctxb) = Headers.toByteBuf(msg.headers)
    out.writeInt(clen)
    out.writeBytes(ctxb)


    out.writeInt(msg.payload.length)
    out.writeBytes(msg.payload)

  }


}



object Pipelines {
  def clientPipeline(pipeline: ChannelPipeline)  = {
    pipeline.addLast(new TMSGEncoder())
    pipeline.addLast(new RMSGDecoder())
  }

  def serverPipeline(pipeline: ChannelPipeline) = {
    pipeline.addLast(new TMSGDecoder())
    pipeline.addLast(new RMSGEncoder())
  }

}
