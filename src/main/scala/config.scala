package magellan

import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.epoll.{EpollEventLoopGroup, Epoll}

import com.twitter.jvm.numProcs
import java.util.concurrent.{Executor, Executors}
import com.twitter.concurrent.NamedPoolThreadFactory


case class WorkerPool(group: EventLoopGroup) {

  def this(executor: Executor, numWorkers: Int) = this(
    if (EpollNative.enabled) new EpollEventLoopGroup(numWorkers, executor)
    else new NioEventLoopGroup(WorkerPool.defaultSize, executor))

}


object WorkerPool {


  def make(): WorkerPool = {

    new WorkerPool(
      Executors.newCachedThreadPool(
        new NamedPoolThreadFactory("finagle/netty4", makeDaemons = true)
      ),
      defaultSize

    )

  }



  def defaultSize() = math.max(8, (numProcs() * 2).ceil.toInt)

}


object EpollNative {
  val enabled = Epoll.isAvailable
}


