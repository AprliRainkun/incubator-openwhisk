package org.apache.openwhisk.core.database.etcd.tests

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.TestSource
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.{TestKit, TestProbe}
import com.google.protobuf.ByteString
import org.apache.openwhisk.core.database.etcd.Utils.rangeEndOfPrefix
import org.apache.openwhisk.grpc.etcd.WatchRequest.RequestUnion
import org.apache.openwhisk.grpc.etcd._
import org.apache.openwhisk.grpc.mvccpb.Event
import org.junit.runner.RunWith
import org.scalatest.OptionValues._
import org.scalatest.junit.JUnitRunner
import org.scalatest.{AsyncWordSpecLike, BeforeAndAfterAll, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

@RunWith(classOf[JUnitRunner])
class EtcdClientTests
    extends TestKit(ActorSystem("EtcdClientTest"))
    with AsyncWordSpecLike
    with Matchers
    with BeforeAndAfterAll {
  private val local = "127.0.0.1"
  private val port = 2379
  private val clientConfig = GrpcClientSettings.connectToServiceAt(local, port).withTls(false)
  private val entityPrefix = "__unit_test_temp_objects/"

  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  "etcd kv client" should {
    "eventually complete a simple put" in {
      val key = (entityPrefix + "simplePut").toByteString
      val req = PutRequest(key = key, value = ByteString.EMPTY)

      kvClient.put(req) map { _ =>
        succeed
      }
    }

    "put a key and then be able to retrieve it" in {
      val key = (entityPrefix + "putThenRetrieve").toByteString
      val value = "vaLUE".toByteString

      for {
        _ <- kvClient.put(PutRequest(key = key, value = value))
        r <- kvClient.range(RangeRequest(key = key))
      } yield {
        r.count should be(1)
        r.kvs.head.value should be(value)
      }
    }
  }

  it should {
    "correctly handle prefix get with trailing slash" in {
      val prefix = entityPrefix + "trailingSlash/"

      val data = (0 to 2) map { i =>
        (prefix + i.toString, s"value$i")
      }

      for {
        _ <- Future.traverse(data) {
          case (k, v) =>
            val req = PutRequest(key = k.toByteString, value = v.toByteString)
            kvClient.put(req)
        }
        req = RangeRequest(key = prefix.toByteString, rangeEnd = rangeEndOfPrefix(prefix.toByteString))
        resp <- kvClient.range(req)
      } yield {
        val (foundKeys, foundValues) = resp.kvs.map(kv => (kv.key.toStringUtf8, kv.value.toStringUtf8)).unzip

        foundKeys should be(data.map(_._1))
        foundValues should be(data.map(_._2))
      }
    }
  }

  "etcd watch client" should {
    "observe sequential key changes" in {
      val key = (entityPrefix + "watchObserve").toByteString

      val (reqProbe, source) = feedAndSource[WatchRequest]
      val respProbe = TestProbe()

      watchClient
        .watch(source)
        .to(Sink.actorRef(respProbe.ref, "completed"))
        .run()

      val create = WatchRequest(RequestUnion.CreateRequest(WatchCreateRequest(key = key)))
      reqProbe.sendNext(create)

      // setup watch
      val rc = respProbe.expectMsgType[WatchResponse](3.seconds)
      val watchId = rc.watchId
      rc.created should be(true)

      // sequential puts
      for (cid <- 0 to 3) {
        val v = s"vaLUE_$cid".toByteString
        kvClient.put(PutRequest(key = key, value = v))
        val r = respProbe.expectMsgType[WatchResponse](3.seconds)

        r.events.size should be(1)

        val e = r.events.head
        e.`type` should be(Event.EventType.PUT)
        e.kv.value.key should be(key)
        e.kv.value.value should be(v)
      }

      // cancel watch
      val cancel = WatchRequest(RequestUnion.CancelRequest(WatchCancelRequest(watchId)))
      reqProbe.sendNext(cancel)
      val ru = respProbe.expectMsgType[WatchResponse](3.seconds)
      ru.canceled should be(true)

      reqProbe.sendComplete()
      succeed
    }
  }

  private def feedAndSource[T] = {
    val (source, sink) = Source
      .asSubscriber[T]
      .toMat(Sink.asPublisher(false))(Keep.both)
      .mapMaterializedValue {
        case (sub, pub) => (Source.fromPublisher(pub), Sink.fromSubscriber(sub))
      }
      .run()
    val feed = TestSource
      .probe[T]
      .toMat(sink)(Keep.left)
      .run()
    (feed, source)
  }

  override def afterAll(): Unit = {
    val prefix = entityPrefix.toByteString
    val req = DeleteRangeRequest(key = prefix, rangeEnd = rangeEndOfPrefix(prefix))
    val fut = kvClient.deleteRange(req) map { _ =>
      system.terminate()
    }
    Await.result(fut, 5.seconds)
  }

  private def kvClient = KVClient(clientConfig)

  private def watchClient = WatchClient(clientConfig)

  implicit private class StringExt(str: String) {
    def toByteString: ByteString = ByteString.copyFromUtf8(str)
  }

}
