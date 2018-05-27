package enkidu

import org.scalatest.FunSuite
import io.netty.channel.embedded.EmbeddedChannel
import enkidu.mux_proto._
import com.twitter.concurrent.{AsyncQueue}

import com.twitter.util.{Await}
import io.netty.channel
import io.netty.bootstrap.{ServerBootstrap, Bootstrap}
import java.net.InetSocketAddress

class CodecSuite extends FunSuite {
  val tchan = new EmbeddedChannel( new TMSGEncoder(), new TMSGDecoder() )

  val tmsg = TMSG(
    List("foo", "bar", "baz"),
    List( ("Fuck", "You"), ("U", "R Gay") ),
    "cuck bitch nigga".getBytes("UTF-8")
  )

  test("Test TMSG") {
    tchan.writeInbound(tmsg)
    val got = tchan.readInbound().asInstanceOf[TMSG]
    assert(got != null)
    val (e, gotm) = (new String(tmsg.payload, "UTF-8") , new String(got.payload, "UTF-8") )

    assert(e == gotm)
  }

  test("Large Body") {
    val payload = Array.fill(300000)((scala.util.Random.nextInt(256) - 128).toByte)

    val bigTMSG = TMSG(
      List("foo", "bar", "baz"),
      List( ("Fuck", "You"), ("U", "R Gay") ),
      payload
    )

    tchan.writeInbound(bigTMSG)
    val bigTMSG1 = tchan.readInbound().asInstanceOf[TMSG]
    println(bigTMSG1.payload.length) 
    assert(bigTMSG1.payload.length == 300000) 
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


}
