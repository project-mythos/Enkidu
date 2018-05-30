package Enkidu

import io.netty.channel.{Channel, ChannelPipeline, ChannelInitializer, ServerChannel}
import io.netty.channel.socket.SocketChannel
import io.netty.bootstrap.{ServerBootstrap}
import com.twitter.util.Future

import io.netty.channel.nio.NioEventLoopGroup
import com.twitter.concurrent.NamedPoolThreadFactory
import java.net.SocketAddress





object Listener {
  type Handler[In, Out] = Flow[In, Out] => Unit

  def makeBridge[In, Out](handler: Handler[In, Out]) = {
    def toFlow(ch: Channel) = Flow.cast[In, Out]( new ChannelFlow(ch) )
    new ServerBridge(toFlow, handler)
  }

  def channelInit[In, Out](
    initPipeline: ChannelPipeline => Unit,
    handler: Handler[In, Out]
  ) = new ChannelInitializer[Channel] {


    def initChannel(ch: Channel) = {
      val bridge = makeBridge(handler)
      val pipeline = ch.pipeline()

      initPipeline(pipeline)
      pipeline.addLast(bridge)
    }

  }


  def bridgeBootstrap[In, Out](
    b: ServerBootstrap,
    initPipeline: ChannelPipeline => Unit,
    handler: Handler[In, Out]
  ) = {

    b.childHandler( channelInit(initPipeline, handler) ) 
  }



}





class Listener[In, Out](
  initPipeline: ChannelPipeline => Unit,
  pool: WorkerPool,
  channelT: Class[_ <: ServerChannel]
) {



  def listen[In, Out](addr: SocketAddress)(handler: Listener.Handler[In, Out]) = {
    val bossLoop = new NioEventLoopGroup(
          1,
          new NamedPoolThreadFactory("finagle/netty4/boss", makeDaemons = true)
        )

    val b = new ServerBootstrap()

    b
      .group(bossLoop, pool.group)
      .channel(channelT)
      .childHandler( Listener.channelInit(initPipeline, handler) )


    b.bind(addr).sync()
  }

}
