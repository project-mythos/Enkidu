package Enkidu
import com.twitter.concurrent.{AsyncSemaphore, Permit}
import java.util.concurrent.{ConcurrentLinkedQueue}
import com.twitter.util.{Future}

import java.util.concurrent.atomic
import atomic.{AtomicBoolean, AtomicInteger}

import collection.JavaConversions._

/*
import stormpot.{Slot, Poolable, Allocator}
import com.twitter.util.Await

case class PFlow[I, O](flow: Flow[I, O], slot: Slot) extends Poolable {

  def release() = {
    slot.release(this) 
  }

  def get() = flow  
}



class FlowAllocator[I, O](
  make: () => Flow[I, O]
) extends Allocator[ Flow[I, O] ] {

  def allocate(slot: Slot) = {
    val f = make() map {x => PFlow(x, slot) }
    Await.result(f)
  }

  def deallocate(item: PFlow) = {
    Await.result( PFlow.get.close() )  
  }

}
 
*/




trait Allocator[T] {
  def close(t: T): Future[Unit]
  def create(): Future[T]
  def check(t: T): Boolean 
}





class Pool[T](A: Allocator[T], min: Int, max: Int) {

  val created = new AtomicInteger(0)
  val sem = new AsyncSemaphore(max)

  val pool = new ConcurrentLinkedQueue[T]()


  def should_make() = {
    (pool.size == 0 && created.get < max) 
  }



  Future.collect {
    (1 to min).toVector.map {x =>
      create_item map {x => pool.offer(x)}
    }
  } 


  def create_item(): Future[T] = {
    A.create() onSuccess {_ => created.incrementAndGet}
  }


  def acquire(): Future[T] = {


    val item = {
      if (should_make) {create_item() }
      else { Future.value( pool.poll() )  }
    }

    item flatMap {x =>

      if (x != null) Future.value(x)
      else acquire()
    }
  }



  def close_item(t: T) = {
    A.close(t) ensure {created.decrementAndGet}
  }


  def release(t: T) = {
    if ( A.check(t) ) { pool.offer(t); }
    else A.close(t)
  }


  def use[U](fn: T => Future[U] ) = {

    sem.acquire.flatMap { permit =>

      acquire() flatMap { t =>

        fn(t) ensure {
          release(t)
          permit.release()
        }

      }
    }


  }



  def close() = {
    pool.foreach {x =>
      A.close(x)
    }
    Future.Done
  }


}
