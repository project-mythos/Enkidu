package Enkidu

import org.scalatest.FunSuite
import io.netty.channel.embedded.EmbeddedChannel
import Enkidu.Mux._
import com.twitter.concurrent.{AsyncQueue}

import com.twitter.util.{Await}
import Enki.KeyHasher._

import io.netty.channel
import io.netty.bootstrap.{ServerBootstrap, Bootstrap}
import java.net.InetSocketAddress


object EmbeddedUtil {

  def write(ch: EmbeddedChannel, t: Object) = {
    ch.writeOutbound(t)
    ch.writeInbound(ch.readOutbound)
  }

  def read[T](ch: EmbeddedChannel) = {
    ch.readInbound().asInstanceOf[T]
  }
}


class CodecSuite extends FunSuite {
  val tchan = new EmbeddedChannel( new TMSGEncoder(), new TMSGDecoder() )

 
  val tmsg = TMSG(
    List("foo", "bar", "baz"),
    List( ("Fuck", "You"), ("U", "R Gay") ),
    "cuck bitch nigga".getBytes("UTF-8")
  )

  test("Test TMSG") {

    EmbeddedUtil.write(tchan, tmsg)
    val got = EmbeddedUtil.read[TMSG](tchan)

    assert(got != null)
    val (e, gotm) = (new String(tmsg.payload, "UTF-8") , new String(got.payload, "UTF-8") )

    assert(e == gotm)
  }

  test("Large Body") {
    val payload = Array.fill(100000)((scala.util.Random.nextInt(256) - 128).toByte)

    val bigTMSG = TMSG(
      List("foo", "bar", "baz"),
      List( ("Fuck", "You"), ("U", "R Gay") ),
      payload
    )

    EmbeddedUtil.write( tchan, (bigTMSG) )
    val bigTMSG1 = EmbeddedUtil.read[TMSG](tchan)

    assert(bigTMSG1.payload.length == 100000)
    val same = FNV1A_64.hashKey(bigTMSG.payload) == FNV1A_64.hashKey(bigTMSG1.payload)
    assert(same)
  }

}







class TransportSuite extends FunSuite {
  val ch = new EmbeddedChannel( new TMSGEncoder(), new TMSGDecoder() )
  val trans = new ChannelFlow(ch)

  test ("Transport Operations") {


    val msg = TMSG(
      List("foo", "bar", "baz"),
      List( ("Fuck", "You"), ("U", "R Gay") ),
      "cuck bitch nigga".getBytes("UTF-8")
    )


    ch.writeInbound(msg)
    val f = trans.read()
    val got = Await.result(f).asInstanceOf[TMSG]

    val (e, gotm) = (new String(msg.payload, "UTF-8") , new String(got.payload, "UTF-8") )
    assert(e == gotm)
  }


  test ("Test RMSG") {
    val rchan = new EmbeddedChannel( new TMSGEncoder(), new TMSGDecoder() )
    val flow = Flow.cast[RMSG, RMSG](new ChannelFlow(rchan) )
    val msg = RMSG(Headers.empty, "nigga fag cunt".getBytes("UTF-8") )

    rchan.writeInbound(msg)

    val got  = Await.result( flow.read )
    assert(msg == got)
  }


}
