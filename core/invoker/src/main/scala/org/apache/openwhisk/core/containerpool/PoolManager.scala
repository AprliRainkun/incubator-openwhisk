package org.apache.openwhisk.core.containerpool

import akka.NotUsed
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.grpc.GrpcClientSettings
import akka.stream._
import akka.stream.scaladsl.{Flow, Source}
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.connector.MessageProducer
import org.apache.openwhisk.core.database._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.grpc.WindowAdvertisement.Message
import org.apache.openwhisk.grpc._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

object PoolManager {
  final case class AllocateContainer(action: String)

  def props(entityStore: ArtifactStore[WhiskEntity], activationStore: ActivationStore, producer: MessageProducer)(
    implicit instance: InvokerInstanceId,
    logging: Logging) = Props(new PoolManager(entityStore, activationStore, producer))

  private case class StreamReady(action: String,
                                 activations: Source[String, NotUsed],
                                 initCommand: AllocateContainer,
                                 requester: ActorRef)
}

class PoolManager(entityStore: ArtifactStore[WhiskEntity], activationStore: ActivationStore, producer: MessageProducer)(
  implicit instance: InvokerInstanceId,
  logging: Logging)
    extends Actor {
  import PoolManager._
  implicit val sys: ActorSystem = context.system
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ex: ExecutionContext = sys.dispatcher

  private val clientSettings = GrpcClientSettings.connectToServiceAt("127.0.0.1", 8081)
  private val queueServiceClient = QueueServiceClient(clientSettings)

  private var pools = Map.empty[String, ActorRef]

  override def receive: Receive = {
    case command @ AllocateContainer(action) =>
      if (!pools.contains(action)) {
        val messageBroker = sys.actorOf(MessageBroker.props(queueServiceClient, action, 5))
        val pool =
          sys.actorOf(ContainerPoolForAction.props(messageBroker, action, entityStore, activationStore, producer))
        pools += (action -> pool)
      }
      pools(action) forward command
  }

  private def establishActivationStream(queueClient: QueueServiceClient, action: String)(
    implicit mat: Materializer): Future[Source[String, NotUsed]] = {
    val (sendFuture, batchSizes) = Source
      .fromGraph(new ConflatedTickerStage)
      .throttle(1, 20 millis)
      .map(b => WindowAdvertisement(Message.WindowsSize(b)))
      .preMaterialize()

    val windows = Source(List(WindowAdvertisement(Message.ActionName(action))))
      .concat(batchSizes)

    val activations = queueClient
      .fetch(windows)
      .map(w => w.activation.head.body)

    sendFuture map { send =>
      val sideChannel = Flow
        .fromGraph(new SideChannelBackpressureStage[String](send))
        .async
        // TODO: make the buffer size configurable
        .addAttributes(Attributes.inputBuffer(5, 5))

      activations.via(sideChannel)
    }
  }
}
