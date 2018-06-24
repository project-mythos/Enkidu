package Enkidu

import com.twitter.util.Future
import io.netty.bootstrap.Bootstrap
import scala.collection.concurrent.TrieMap


import io.netty.channel
import com.twitter.util.{Future, Promise}
import java.net.SocketAddress
import channel.ChannelOption
import java.lang.{Boolean => JBool}

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
  endpoints: EndpointPool.PoolMap[In, Out], 
  min: Int,
  max: Int
) extends ConnectionManager[In, Out] {



  def makePool(node: Node) = {

    val allocator = new Allocator[Flow[In, Out]] {
      def close(f: Flow[In, Out] ) = f.close()
      def create() = Connection.connect[In, Out](BS, node)
      def check(flow: Flow[In, Out]) = flow.closed() 
    }

    new Pool(allocator, min, max)
  }



  def addEndpoint(endpoint: Node) = {
    val pool = makePool(endpoint) 
    endpoints.putIfAbsent(endpoint, pool)
    pool
  }


  def removeEndpoint(endpoint: Node): Future[Unit] = {
    endpoints.get(endpoint) match {
      case Some(pool) => pool.close()
      case _ => Future.Done
    }
  }




  def connect[T](peer: Node)(f: Flow[In, Out] => Future[T]) = {


    val exists = endpoints.get(peer).isDefined

    val p = {
      if (exists) endpoints.get(peer).get
      else addEndpoint(peer) 
    }

    p.use {flo => f(flo) }

  }


}




object EndpointPool {

  type FlowPool[I, O] = Pool[ Flow[I, O] ]
  type PoolMap[In, Out] = TrieMap[Node, FlowPool[In, Out] ]

  def apply[In, Out](bs: Bootstrap): EndpointPool[In, Out] = {

    bs.option[JBool](ChannelOption.SO_KEEPALIVE, true)
    val TM = TrieMap[Node, FlowPool[In, Out] ]()

    val max_size = 100
    val min_size = 10

    new EndpointPool(bs, TM, max_size, min_size)
  }


}
 
