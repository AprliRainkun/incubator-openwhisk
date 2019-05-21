package org.apache.openwhisk.core.containerpool

import akka.NotUsed
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.stream._
import akka.stream.scaladsl.Source
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.connector.MessageProducer
import org.apache.openwhisk.core.database._
import org.apache.openwhisk.core.database.etcd.QueueMetadataStore
import org.apache.openwhisk.core.entity.ExecManifest.ImageName
import org.apache.openwhisk.core.entity._

import scala.concurrent.{ExecutionContext, Future}

final case class ContainerOperationError(msg: String)

object PoolManager {
  final case class AllocateContainer(tid: TransactionId, actionEntity: DocInfo, number: Int)
  type ContainerOperationResult = Either[ContainerOperationError, Unit]

  def props(factory: (TransactionId, String, ImageName, Boolean, ByteSize, Int) => Future[Container],
            entityStore: ArtifactStore[WhiskEntity],
            activationStore: ActivationStore,
            poolConfig: ContainerPoolConfig,
            queueMetadataStore: QueueMetadataStore,
            producer: MessageProducer)(implicit instance: InvokerInstanceId, logging: Logging) =
    Props(new PoolManager(factory, entityStore, activationStore, poolConfig, queueMetadataStore, producer))

  private case class StreamReady(action: String,
                                 activations: Source[String, NotUsed],
                                 initCommand: AllocateContainer,
                                 requester: ActorRef)
}

class PoolManager(factory: (TransactionId, String, ImageName, Boolean, ByteSize, Int) => Future[Container],
                  entityStore: ArtifactStore[WhiskEntity],
                  activationStore: ActivationStore,
                  poolConfig: ContainerPoolConfig,
                  queueMetadataStore: QueueMetadataStore,
                  producer: MessageProducer)(implicit instance: InvokerInstanceId, logging: Logging)
    extends Actor {
  import PoolManager._
  implicit val sys: ActorSystem = context.system
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ex: ExecutionContext = sys.dispatcher

  private var pools = Map.empty[DocInfo, ActorRef]

  override def receive: Receive = {
    case command @ AllocateContainer(_, entity, _) =>
      if (!pools.contains(entity)) {
        val messageBroker = sys.actorOf(MessageBroker.props(entity, 5, queueMetadataStore))
        val pool =
          sys.actorOf(
            ContainerPoolForAction.props(messageBroker, factory, entityStore, activationStore, poolConfig, producer))
        pools += (entity -> pool)
        logging.info(this, s"pool and message broker created for action $entity")
      }
      pools(entity) forward command
  }
}
