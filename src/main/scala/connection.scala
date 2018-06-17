package Enkidu

import com.twitter.util.Future
import io.netty.bootstrap.Bootstrap
import Enki.Pool
import scala.collection.concurrent.TrieMap


import io.netty.channel
import com.twitter.util.{Future, Promise}
import java.net.SocketAddress


object Connection {
  import channel.{ChannelInitializer, Channel, ChannelFuture}
  import io.netty.bootstrap.Bootstrap


  def connect[Req, Rep](b: Bootstrap, addr: SocketAddress) = {

    FutureConversion.toFuture( b.connect(addr) ) {
      f: ChannelFuture => Flow.cast[Req, Rep](new ChannelFlow(f.channel) )
    }

  }


  def connect[Req, Rep](b: Bootstrap, addr: Node) = {
    FutureConversion.toFuture( b.connect(addr.toSocketAddress) ) {
      f: ChannelFuture => Flow.cast[Req, Rep](new ChannelFlow(f.channel) )
    }
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






trait ConnectionManager[In, Out] {
  def connect[T](addr: Node)(fn: Flow[In, Out] => Future[T]): Future[T]
}


class SimpleCM[In, Out](BS: Bootstrap) extends ConnectionManager[In, Out] {

  def connect[T](addr: Node)(fn: Flow[In, Out] => Future[T]): Future[T] = {
    Connection.connect[In, Out](BS, addr.toSocketAddress) flatMap{flow =>
      fn(flow) ensure { flow.close() }
    }
  }
 
}



class EndpointPool[In, Out](
  BS: Bootstrap,
  sizePerEndpoint: Int, 
  endpoints: TrieMap[Node, Pool[Flow[In, Out] ] ]
) extends ConnectionManager[In, Out] {



  def create_conn(peer: Node)() = {
    Connection.connect[In, Out](BS, peer)
  }

  def close_conn(flow: Flow[In, Out]) = flow.close()
  def check(flow: Flow[In, Out]) = (flow.closed() != true)

  def makePool(peer: Node) = {
     Pool.make(sizePerEndpoint, create_conn(peer), check, close_conn)
  }

  def addEndpoint(endpoint: Node) = {
    val pool = Pool.make(sizePerEndpoint, create_conn(endpoint), check, close_conn)
    endpoints.putIfAbsent(endpoint, pool)
  }


  def removeEndpoint(endpoint: Node): Future[Unit] = {
    endpoints.get(endpoint) match {
      case Some(pool) => Pool.destroy(pool)
      case _ => Future.Done
    }
  }



  def connect[T](peer: Node)(f: Flow[In, Out] => Future[T]) = {

    def make = makePool(peer)

    val p = endpoints.getOrElseUpdate(peer, make)
    Pool.use(p)(f)
  }


}
