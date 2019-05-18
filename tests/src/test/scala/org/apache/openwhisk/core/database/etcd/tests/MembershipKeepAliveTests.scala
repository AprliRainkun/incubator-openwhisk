package org.apache.openwhisk.core.database.etcd.tests

import akka.actor.PoisonPill
import akka.testkit.TestProbe
import org.apache.openwhisk.core.database.etcd._
import org.apache.openwhisk.grpc.etcd._
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

class MembershipKeepAliveTests extends TestBase("MembershipKeepAliveTests") with Eventually {
  lazy val etcdHost = "127.0.0.1"
  lazy val etcdPort = 2379

  "Membership keep-alive service" should {
    "write kv to database" in {
      val keepAlive = system.actorOf(MembershipKeepAlive.props(5, metadataStoreConfig))
      val probe = TestProbe()
      val key = metadataStoreConfig.schedulerEndpointKeyTemplate.format(0)
      val value = "test value"

      probe.send(keepAlive, MembershipKeepAlive.SetData(key, value))

      eventually(Timeout(3 seconds), Interval(500 millis)) {
        val valueFut = getStringValue(key)
        val found = Await.result(valueFut, 1 second)
        found should be(value)
      }
      succeed
    }

    "maintain membership" in {
      val failureSeconds = 2
      val keepAlive = system.actorOf(MembershipKeepAlive.props(failureSeconds, metadataStoreConfig))
      val probe = TestProbe()
      val key = metadataStoreConfig.schedulerEndpointKeyTemplate.format(1)
      val value = "test value"

      probe.send(keepAlive, MembershipKeepAlive.SetData(key, value))

      for {
        // wait for database update
        _ <- Future { Thread.sleep(500) }
        v1 <- getStringValue(key)
        _ = v1 should be(value)
        // wait for timeout
        _ <- Future { Thread.sleep(failureSeconds * 2000) }
        v2 <- getStringValue(key)
        _ = v2 should be(value)
      } yield succeed
    }

    "delete etcd entry when failed" in {
      val failureSeconds = 2
      val keepAlive = system.actorOf(MembershipKeepAlive.props(failureSeconds, metadataStoreConfig))
      val probe = TestProbe()
      val key = metadataStoreConfig.schedulerEndpointKeyTemplate.format(2)
      val value = "test value"

      probe.send(keepAlive, MembershipKeepAlive.SetData(key, value))

      for {
        // wait for database update
        _ <- Future { Thread.sleep(500) }
        v1 <- getStringValue(key)
        _ = v1 should be(value)
        // stop keep alive pings
        _ = probe.send(keepAlive, PoisonPill)
        _ <- Future { Thread.sleep(failureSeconds * 2000) }
        timeout = getStringValue(key)
      } yield {
        an[IllegalStateException] should be thrownBy Await.result(timeout, 2 seconds)
      }
    }
  }

  private def getStringValue(key: String): Future[String] = {
    val req = RangeRequest(key.toByteString)
    kvClient.range(req) flatMap { resp =>
      resp.count match {
        case 1 => Future.successful(resp.kvs.head.value.toStringUtf8)
        case _ => Future.failed(new IllegalStateException("value count is not 1"))
      }
    }
  }
}
