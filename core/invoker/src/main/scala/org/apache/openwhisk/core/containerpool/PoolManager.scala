package org.apache.openwhisk.core.containerpool

import akka.NotUsed
import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.grpc.GrpcClientSettings
import akka.stream._
import akka.stream.scaladsl.Source
import org.apache.openwhisk.grpc.WindowAdvertisement.Message
import org.apache.openwhisk.grpc._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

class PoolManager() extends Actor {
  import PoolManager._
  implicit val sys: ActorSystem = context.system
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ex: ExecutionContext = sys.dispatcher

  private val clientSettings = GrpcClientSettings.connectToServiceAt("127.0.0.1", 8081)
  private val queueServiceClient = QueueServiceClient(clientSettings)

  private var pools = Map.empty[String, ActorRef]

  override def receive: Receive = {
    case DummyAllocateContainer(action) =>
      if (!pools.contains(action)) {
        val (senderFut, activations) = establishActivationFlow(queueServiceClient, action)
        val poolActor = sys.actorOf(ContainerPoolForAction.props(senderFut, activations))
        pools += (action -> poolActor)
      }
      sender ! Status.Success
  }

  private def establishActivationFlow(queueClient: QueueServiceClient, action: String)(
  implicit mat: Materializer): (Future[TickerSendEnd], Source[Array[Byte], NotUsed]) = {
    val (send, batchSizes) = Source
      .fromGraph(new ConflatedTickerStage)
      .throttle(1, 20.millis)
      .map(b => WindowAdvertisement(Message.WindowsSize(b)))
      .preMaterialize()

    val windows = Source(List(WindowAdvertisement(Message.ActionName(action))))
      .concat(batchSizes)
    val activations = queueClient
      .fetch(windows)
      .map(_ => ???)

    (send, activations)
  }
}

object PoolManager {
  final case class DummyAllocateContainer(action: String)

  def props() = Props(new PoolManager)
}
