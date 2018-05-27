package enkidu

import io.netty.channel
import com.twitter.util.{Future, Promise}
import java.net.SocketAddress

object Connection {
  import channel.{ChannelInitializer, Channel, ChannelFutureListener, ChannelException}
  import io.netty.bootstrap.Bootstrap


  def fromBootstrap[Req, Rep](
    addr: SocketAddress,
    b: Bootstrap,
    makeFlow: Channel => Flow[Any, Any]
  ): Future[ Flow[Req, Rep] ]= {

    val p = Promise[Flow[Req, Rep] ]()

    b.connect(addr).addListener( new ChannelFutureListener {

      def operationComplete(f: channel.ChannelFuture) = {
        if (f.isSuccess) {

          val t = (makeFlow andThen Flow.cast[Req, Rep])(f.channel())
          p.setValue(t)

        } else {
          p.setException( new ChannelException() )
        }

      }

    })

    p
  }




  def make[Req, Rep](
    c: java.lang.Class[_ <: Channel], 
    addr: SocketAddress,
    initializer: channel.ChannelInitializer[Channel],
    workers: WorkerPool, 
    makeFlow: Channel => Flow[Any, Any]

  ): Future[Flow[Req, Rep]] = {

    val p = new Promise[Flow[Any, Any] ]
    val b = new Bootstrap()

    b
      .group(workers.group)
      .channel(c)
      .handler(initializer)

    fromBootstrap[Req, Rep](addr, b, makeFlow)
  }



}
