package Enkidu.Loadbalancer

import org.scalatest._
import Enkidu.Node
import com.twitter.util.{Future, RandomSocket, Await, Var}


object Util {

  def makeLoadedNode() = {
    val n = Node("localhost", RandomSocket.nextPort)
    LoadedNode(n)
  }

  def makeNode() = {
    Node("localhost", RandomSocket.nextPort)
  }

}


class DistributorSpec extends FunSuite {

  
  val lhost = "localhost"
  val higher = Util.makeLoadedNode
  higher.incr

  val vec = Vector(Util.makeLoadedNode, higher)


  test ("pick the least loaded node") {
    assert( P2C.pick(vec) == vec.head)
  }



  test ("decrement the selected nodes load after it is used") {
    val f = P2C.use(vec){ln => Future.Done} map {x =>
      val b = vec.head.load == 0
      assert(b)
    }

    Await.result(f)
  }


  test ("display changes in load") {
    vec.head.incr
    assert(vec.head.load == 1)

    vec.head.decr
    assert(vec.head.load == 0)
  }

}



class ServerListSpec extends FunSuite {
  val hosts = List.fill(10)(Util.makeNode).toSet
  val SL = new ServerList(Var(hosts))



  test("contains all hosts") {
    SL.update(hosts)
    val se = SL.endpoints.map {x => x.toNode}.toSet
    assert(hosts == se)
  }



  test("keeps load info on update") {
    

    SL.endpoints.head.incr()
    SL.update(hosts)

    synchronized { assert(SL.endpoints.head.load() == 1) }
  }

  test ("discard dead hosts") {
    
    val nl = hosts.toList.take(5).toSet
    SL.endpoints.head.incr()

    SL.update(nl)


    val cond = SL.endpoints.exists(x => x.toNode == hosts.toList(6) ) != true
    assert(cond)
  }


}
