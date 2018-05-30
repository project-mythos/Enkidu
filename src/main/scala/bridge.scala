package Enkidu

import io.netty.channel.{
  ChannelInboundHandlerAdapter, ChannelHandlerContext, Channel
}
import io.netty.channel.ChannelHandler.Sharable

@Sharable
class ServerBridge[In, Out](
  makeFlow: Channel => Flow[In, Out],
  serveFlow: Flow[In, Out] => Unit
) extends ChannelInboundHandlerAdapter {

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    val transport = makeFlow(ctx.channel())
    serveFlow(transport)
  }


}

