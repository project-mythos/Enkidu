package Enkidu

import io.netty.channel
import com.twitter.util.{Future, Promise}
import java.net.SocketAddress


object Connection {
  import channel.{ChannelInitializer, Channel, ChannelFutureListener, ChannelException}
  import io.netty.bootstrap.Bootstrap


  def connect[Req, Rep](b: Bootstrap, addr: SocketAddress) = {

    val p = new Promise[Flow[Req, Rep] ] 
    val makeFlow = {ch: Channel => new ChannelFlow(ch) }
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





  def send_recv[Req, Rep](trans: Flow[Req,  Rep], req: Req): Future[Rep] = {
    val p = Promise[Rep]()
    trans.write(req) flatMap { x => trans.read() } respond (rep => p.updateIfEmpty(rep))
  }


  def fire_forget[Req, Rep](conn: Flow[Req, Rep], msg: Req) = {
    conn.write(msg)
  }





  def bootstrap[Req, Rep](
    c: java.lang.Class[_ <: Channel], 
    initializer: channel.ChannelInitializer[Channel],
    workers: WorkerPool
  ): Bootstrap = {

    val p = new Promise[Flow[Any, Any] ]
    val b = new Bootstrap()

    b
      .group(workers.group)
      .channel(c)
      .handler(initializer)

    b
  }



}
