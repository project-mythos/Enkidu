package Enkidu.Loadbalancer

import Enkidu.{Node, ConnectionManager, Flow}
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import com.twitter.util.{Var, Updatable, Future, Witness}
import Enki.KeyHasher
import KeyHasher.{FNV1A_64, MURMUR3}
import Enki.CHash
import scala.util.Random


case class LoadedNode(node: Node) {

  private val counter = new AtomicLong(0L)

  def load() = counter.get
  def incr() = counter.incrementAndGet()
  def decr() = counter.decrementAndGet()

  def toNode: Node = node
  def id() = node.id

}




trait Distributor {
  def pick(vector: Vector[LoadedNode] ): LoadedNode


  def use[T](vector: Vector[LoadedNode])(fn: LoadedNode => Future[T] ): Future[T] = {
    val n = pick(vector)
    n.incr
    fn(n) ensure n.decr
  }


}



trait KeyDistributor {
  def pick(key: Array[Byte], v: Vector[LoadedNode]): LoadedNode

  def use[T](key: Array[Byte], v: Vector[LoadedNode])(f: LoadedNode => Future[T]): Future[T] = {
    val n = pick(key, v)
    n.incr
    f(n) ensure n.decr 
  }


}




object P2C extends Distributor {

  def pick(nodes: Vector[LoadedNode]) = {

    val a = Random.nextInt(nodes.size) 
    val b = Random.nextInt(nodes.size) 

    val (c1, c2) = (nodes(a), nodes(b) )
    if (c1.load <= c2.load) c1
    else c2
  }

}





object P2CPKG extends KeyDistributor {
 
  val hasher = FNV1A_64
  val hasher1 = MURMUR3

  def pick(key: Array[Byte], nodes: Vector[LoadedNode]): LoadedNode = {
    val c1 = nodes( hasher.hashKey(key).toInt % nodes.length )
    val c2 = nodes( hasher1.hashKey(key).toInt % nodes.length )


    if (c1.load <= c2.load) c1
    else c2
  }

}


class CHashLeastLoaded(factor: Int, hasher: KeyHasher) extends KeyDistributor {


  val S = CHash.Sharder

  def pick(key: Array[Byte], nodes: Vector[LoadedNode]) = {
    val key1 = hasher.hashKey(key)
    val shards = S.lookup(nodes, key1, factor)
    shards.minBy(x => x.load)
  }

  def shard(key: Array[Byte], nodes: Vector[LoadedNode]) = {
    val key1 = hasher.hashKey(key)
    S.lookup(nodes, key1, factor)
  }


}





class ServerList(base: Set[Node]) {
  private var servers =
    base.map {x => LoadedNode(x)}.toVector.sortWith(_.node.id < _.node.id)

  val membershipWitness = new Witness[Set[Node]] {
    def notify(nodes: Set[Node]) = { update(nodes) }
  }


  def endpoints(): Vector[LoadedNode] = servers

  def update(nodes: Set[Node]) = {

    val e1 = servers.map {ln => ln.node} toSet

    val toRemove = e1 diff nodes

    val toAdd = nodes diff e1 map {x => LoadedNode(x) } toVector

    val v2 = servers filter {x =>
      toRemove.contains( x.node ) != true
    }


    val v3 = (v2 ++ toAdd).sortWith(_.node.id < _.node.id)

    synchronized{ servers = v3 }
    //servers = v3
  }



  def withSource(v: Var[Set[Node]] ) = {
    v.changes.register(update) 
  }

  def get(node: Node) = {
    val fo = servers.find(n => n.node == node)
    fo match {

      case Some(lnode) => lnode

      case None =>

        val ln = LoadedNode(node)
        synchronized {servers = servers :+ ln}
        ln
    }
  }

}




object ServerList {
  type Endpoints = Var[Node] with Updatable[Node]


  def apply(): ServerList =  {
    val empty = Set[Node]()
    new ServerList(empty)
  }


}





class LB[In, Out](
  CM: ConnectionManager[In, Out],
  D: Distributor,
  SL: ServerList
) {

  def use[T](f: Flow[In, Out] => Future[T]) = {

    D.use(SL.endpoints) {ln =>
      CM.connect(ln.toNode)(f)
    }

  }

}



class KeyBasedLB[In, Out](
  CM: ConnectionManager[In, Out],
  KD: KeyDistributor,
  SL: ServerList
) {


  def use[T](key: Array[Byte])(f: Flow[In, Out] => Future[T]) = {

    KD.use(key, SL.endpoints) { ln =>
      CM.connect(ln.toNode)(f)
    }

  }

}
