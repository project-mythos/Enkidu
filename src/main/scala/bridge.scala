package Enkidu

import io.netty.channel.{
  ChannelInboundHandlerAdapter, ChannelHandlerContext, Channel,
  ChannelFuture, ChannelException, ChannelFutureListener
}
import io.netty.channel.ChannelHandler.Sharable
import com.twitter.util.{Future, Promise, Try}

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



object FutureConversion {

  def toFuture[T](cf: ChannelFuture)(fn: ChannelFuture => T): Future[T] = {
    val p = new Promise[T]()


    cf.addListener( new ChannelFutureListener {
      def operationComplete(f: ChannelFuture) = {
        if (f.isSuccess) p.setValue( fn(f) )
        else p.setException( new ChannelException() )
      }
    })

    p
  }

  import io.netty.util.concurrent.{ Future => NettyFuture, FutureListener} 

  def toFuture[T](nf: NettyFuture[T]): Future[T] = {
    val p = new Promise[T]

    nf.addListener( new FutureListener[T] {
      def operationComplete(t: NettyFuture[T]): Unit = {
        p.update( Try(t.getNow) );
      }

    })

    p
  }
    
}
