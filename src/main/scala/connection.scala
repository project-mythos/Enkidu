package magellan
import io.netty.channel
import com.twitter.util.{Future, Promise}
import java.net.SocketAddress

object Connection {
  import channel.{ChannelInitializer, Channel, ChannelFutureListener, ChannelException}
  import io.netty.bootstrap.Bootstrap


  def fromBootstrap[Req, Rep](
    addr: SocketAddress,
    b: Bootstrap,
    makeTransport: Channel => Transport[Any, Any]
  ): Future[ Transport[Req, Rep] ]= {

    val p = Promise[Transport[Req, Rep] ]()

    b.connect(addr).addListener( new ChannelFutureListener {

      def operationComplete(f: channel.ChannelFuture) = {
        if (f.isSuccess) {

          val t = (makeTransport andThen Transport.cast[Req, Rep])(f.channel())
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
    workers: WorkerPool = WorkerPool.make(),
    makeTransport: Channel => Transport[Any, Any]

  ): Future[Transport[Req, Rep]] = {

    val p = new Promise[Transport[Any, Any] ]
    val b = new Bootstrap()

    b
      .group(workers.group)
      .channel(c)
      .handler(initializer)

    fromBootstrap[Req, Rep](addr, b, makeTransport)
  }




}
