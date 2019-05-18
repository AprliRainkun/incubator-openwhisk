package org.apache.openwhisk.core.database.etcd.tests

import akka.stream.testkit.scaladsl.TestSink
import org.apache.openwhisk.core.database.etcd._
import org.apache.openwhisk.grpc.etcd._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

class MembershipWatcherTests extends TestBase("MembershipWatcherTests") {
  lazy val etcdHost = "127.0.0.1"
  lazy val etcdPort = 2379

  "Membership watch service" should {
    "be able to retrieve the previously inserted records" in {
      val watcher = new MembershipWatcher(metadataStoreConfig)
      val watchedPrefix = metadataStoreConfig.schedulerEndpointKeyTemplate.replaceAll("[^/]+$", "")

      val putFut = sequence(1 to 3) { i =>
        val key = metadataStoreConfig.schedulerEndpointKeyTemplate.format(i)
        val value = s"test value for $i"
        put(key, value) map { _ =>
          (key, value)
        }
      }
      val puts = Await.result(putFut, 2 seconds)

      val probe = watcher.watchMembership(watchedPrefix).runWith(TestSink.probe)
      probe.request(3)

      puts foreach {
        case (k, v) =>
          val event = probe.expectNext(1 second)
          event shouldBe an[MembershipEvent.Put]
          val asserted = event.asInstanceOf[MembershipEvent.Put]
          asserted.key should be(k)
          asserted.value should be(v)
      }
      probe.onComplete()

      succeed
    }

    "receive current and all subsequent puts" in {
      val watcher = new MembershipWatcher(metadataStoreConfig)
      val watchPrefix = metadataStoreConfig.invokerEndpointKeyTemplate.replaceAll("[^/]+$", "")

      def simulateJoin(id: Int) = {
        val key = metadataStoreConfig.invokerEndpointKeyTemplate.format(id)
        val value = s"test value for $id"
        put(key, value) map { _ =>
          (key, value)
        }
      }
      val snapshotFut = sequence(1 to 3)(simulateJoin)
      val snapshot = Await.result(snapshotFut, 2 seconds)

      val probe = watcher.watchMembership(watchPrefix).runWith(TestSink.probe)
      probe.request(3)

      snapshot foreach {
        case (k, v) =>
          val event = probe.expectNext(1 second)
          event shouldBe an[MembershipEvent.Put]
          val asserted = event.asInstanceOf[MembershipEvent.Put]
          asserted.key should be(k)
          asserted.value should be(v)
      }

      val subsequentFut = sequence(4 to 6)(simulateJoin)
      val subsequent = Await.result(subsequentFut, 2 seconds)
      probe.request(3)

      subsequent foreach {
        case (k, v) =>
          val event = probe.expectNext(1 second)
          event shouldBe an[MembershipEvent.Put]
          val asserted = event.asInstanceOf[MembershipEvent.Put]
          asserted.key should be(k)
          asserted.value should be(v)
      }
      succeed
    }

    "receive subsequent deletes" in {
      val watcher = new MembershipWatcher(metadataStoreConfig)

      val template = "membership/fake-component/%d"
      val watchPrefix = "membership/fake-component/"

      // first join three components
      val seedFut = sequence(1 to 3) { i =>
        val key = template.format(i)
        val value = s"test value for fake $i"
        put(key, value) map { _ =>
          (key, value)
        }
      }
      val seeds = Await.result(seedFut, 2 seconds)

      val probe = watcher.watchMembership(watchPrefix).runWith(TestSink.probe)
      probe.request(6)
      probe.expectNextN(seeds map {
        case (k, v) => MembershipEvent.Put(k, v)
      })

      // then delete all
      val deleteFut = sequence(1 to 3) { i =>
        val key = template.format(i)
        val req = DeleteRangeRequest(key.toByteString)
        kvClient.deleteRange(req).map(_ => key)
      }
      val deletes = Await.result(deleteFut, 2 seconds)

      probe.expectNextN(deletes.map(MembershipEvent.Delete))

      succeed
    }
  }

  private def put(key: String, value: String): Future[Unit] = {
    val req = PutRequest(key.toByteString, value.toByteString)
    kvClient.put(req) map (_ => ())
  }

  private def sequence[A, B](seed: Seq[A])(f: A => Future[B]): Future[List[B]] =
    seed
      .foldLeft(Future.successful(List.empty[B])) {
        case (prevF, ai) =>
          for {
            p <- prevF
            b <- f(ai)
          } yield b :: p
      }
      .map(_.reverse)
}
