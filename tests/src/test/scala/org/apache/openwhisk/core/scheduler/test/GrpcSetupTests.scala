package org.apache.openwhisk.core.scheduler.test

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.{TestKit, TestProbe}
import org.apache.openwhisk.common.{AkkaLogging, Logging, TransactionId => WhiskTid}
import org.apache.openwhisk.core.scheduler._
import org.apache.openwhisk.grpc._
import org.junit.runner.RunWith
import org.scalatest.OptionValues._
import org.scalatest.junit.JUnitRunner
import org.scalatest.{AsyncWordSpecLike, BeforeAndAfterAll, Matchers}
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

@RunWith(classOf[JUnitRunner])
class GrpcSetupTests
    extends TestKit(ActorSystem("GrpcSetupTest"))
    with AsyncWordSpecLike
    with Matchers
    with BeforeAndAfterAll {
  val local = "127.0.0.1"
  val port = 8899

  implicit val sys: ActorSystem = system
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val logging: Logging = new AkkaLogging(akka.event.Logging.getLogger(sys, this))

  override def beforeAll(): Unit = {
    new QueueServiceServer(new QueueServiceDraftImpl()).run(local, port)
  }

  override def afterAll(): Unit = {
    system.terminate()
  }

  val tid = TransactionId(WhiskTid.schedulerStarting.toJson.compactPrint)
  val actionId = ActionIdentifier("ns/pkg/act", "1")

  "Queue service server" should {
    "expose its fetch method" in {
      import WindowAdvertisement.Message.{Action, WindowsSize}

      val windows = WindowAdvertisement(Action(actionId)) ::
        List.fill(3)(WindowAdvertisement(WindowsSize(1)))
      val activations = client.fetch(Source(windows))
      val probe = TestProbe()

      activations.to(Sink.actorRef(probe.ref, "completed")).run()

      0 to 3 foreach { _ =>
        val resp = probe.expectMsgType[FetchActivationResponse](3.seconds)
        resp.status.map(_.statusCode) should be(Option(200))
      }

      succeed
    }

    "expose its put method" in {
      val act = Activation(Some(tid), Some(actionId))

      val resp = Await.result(client.put(act), 3.seconds)

      resp.status.value.statusCode should be(200)
    }

    "expose its create method" in {
      val act = CreateQueueRequest(Some(tid), Some(actionId))

      val resp = Await.result(client.create(act), 3.seconds)
      resp.status.value.statusCode should be(200)
    }
  }

  private def client = {
    val config = GrpcClientSettings.connectToServiceAt(local, port).withTls(false)
    QueueServiceClient(config)
  }
}
