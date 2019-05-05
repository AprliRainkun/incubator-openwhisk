package org.apache.openwhisk.core.containerpool

import akka.NotUsed
import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.grpc.GrpcClientSettings
import akka.stream._
import akka.stream.scaladsl.{Flow, Source}
import org.apache.openwhisk.grpc.WindowAdvertisement.Message
import org.apache.openwhisk.grpc._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

object PoolManager {
  final case class DummyAllocateContainer(action: String)

  def props() = Props(new PoolManager)

  private case class StreamReady(action: String, activations: Source[Array[Byte], NotUsed], requester: ActorRef)
}

class PoolManager() extends Actor {
  import PoolManager._
  implicit val sys: ActorSystem = context.system
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ex: ExecutionContext = sys.dispatcher

  private val clientSettings = GrpcClientSettings.connectToServiceAt("127.0.0.1", 8081)
  private val queueServiceClient = QueueServiceClient(clientSettings)

  private var pools = Map.empty[String, ActorRef]

  override def receive: Receive = {
    case msg @ DummyAllocateContainer(action) =>
      if (!pools.contains(action)) {
        val activationsFut = establishActivationStream(queueServiceClient, action)
        val currentSender = sender()
        activationsFut map { acts =>
          self ! StreamReady(action, acts, currentSender)
        }
      } else {
        pools(action) forward msg
      }
    case StreamReady(action, activations, requester) =>
      val poolActor = sys.actorOf(ContainerPoolForAction.props(activations))
      pools += (action -> poolActor)
      requester ! Status.Success
  }

  private def establishActivationStream(queueClient: QueueServiceClient, action: String)(
    implicit mat: Materializer): Future[Source[Array[Byte], NotUsed]] = {
    val (sendFuture, batchSizes) = Source
      .fromGraph(new ConflatedTickerStage)
      .throttle(1, 20 millis)
      .map(b => WindowAdvertisement(Message.WindowsSize(b)))
      .preMaterialize()

    val windows = Source(List(WindowAdvertisement(Message.ActionName(action))))
      .concat(batchSizes)

    val activations = queueClient
      .fetch(windows)
      .map(_ => ???)

    sendFuture map { send =>
      val sideChannel = Flow
        .fromGraph(new SideChannelBackpressureStage[Array[Byte]](send))
        .async
        // TODO: make the buffer size configurable
        .addAttributes(Attributes.inputBuffer(5, 5))
      activations.via(sideChannel)
    }
  }
}
