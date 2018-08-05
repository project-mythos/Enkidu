package Enkidu
import org.scalatest._
import Enkidu.Loadbalancer.Util

class NodeCodecSuite extends FunSuite {
  val node = Util.makeNode

  test("Codec doesn't tamper with node value") {
    val got = Node.fromString( node.toString )
    assert(node == got)
  }


  
}
