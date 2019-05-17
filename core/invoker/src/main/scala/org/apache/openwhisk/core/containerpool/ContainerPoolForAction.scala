package org.apache.openwhisk.core.containerpool

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.Timeout
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.database._
import org.apache.openwhisk.core.entity.ExecManifest.ImageName
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.invoker.InvokerReactive
import org.apache.openwhisk.http.Messages

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object ContainerPoolForAction {
  def props(messageBroker: ActorRef,
            factory: (TransactionId, String, ImageName, Boolean, ByteSize, Int) => Future[Container],
            entityStore: ArtifactStore[WhiskEntity],
            activationStore: ActivationStore,
            poolConfig: ContainerPoolConfig,
            producer: MessageProducer)(implicit instance: InvokerInstanceId, logging: Logging) =
    Props(new ContainerPoolForAction(messageBroker, factory, entityStore, activationStore, poolConfig, producer))

  // private data
  private case class ContainerDescriptor(name: String, capacity: Int, action: WhiskAction) {
    def changeCapacity(cap: Int) = copy(capacity = cap)
  }

  // private message
  private case class ActionDocFetched(tid: TransactionId, result: Try[WhiskAction], origin: ActorRef)
}

class ContainerPoolForAction(messageBroker: ActorRef,
                             factory: (TransactionId, String, ImageName, Boolean, ByteSize, Int) => Future[Container],
                             entityStore: ArtifactStore[WhiskEntity],
                             activationStore: ActivationStore,
                             poolConfig: ContainerPoolConfig,
                             producer: MessageProducer)(implicit instance: InvokerInstanceId, logging: Logging)
    extends Actor {
  import ContainerPoolForAction._

  implicit val sys: ActorSystem = context.system
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ex: ExecutionContext = sys.dispatcher
  implicit val timeout: Timeout = Timeout(1 second)

  private var freePool = Map.empty[ActorRef, ContainerDescriptor]
  private var busyPool = Map.empty[ActorRef, ContainerDescriptor]
  private var initingPool = Map.empty[ActorRef, ContainerDescriptor]
  private var retiringPool = Map.empty[ActorRef, ContainerDescriptor]

  override def receive: Receive = {
    case MessageBroker.NewMessage(msg) =>
      // parse data
      ActivationMessage.parse(msg).map { parsed =>
        val (container, d @ ContainerDescriptor(_, cap, _)) = freePool.head
        container ! ContainerProxySlim.Start(parsed)
        if (cap > 1) {
          freePool += (container -> d.changeCapacity(cap - 1))
        } else {
          freePool -= container
          busyPool += (container -> d.changeCapacity(0))
        }
      } recoverWith {
        case t =>
          messageBroker ! MessageBroker.NeedMessage(1)
          logging.error(this, s"terminal failure while processing message $t")
          Success(())
      }
    // construct a runnable action by fetching code from database
    // ack immediately on failure
    // dispatch runnable action to container proxy
    case PoolManager.AllocateContainer(tid, entity) =>
      implicit val transid: TransactionId = tid
      val origin = sender()
      WhiskAction.get(entityStore, entity.id, entity.rev, fromCache = entity.rev != DocRevision.empty) onComplete {
        result =>
          self ! ActionDocFetched(tid, result, origin)
      }

    case ActionDocFetched(tid, result, origin) =>
      result map { action =>
        action.toExecutableWhiskAction match {
          case Some(exe) =>
            val name = ContainerProxy.containerName(instance, action.namespace.asString, action.name.asString)
            val container = context.actorOf(
              ContainerProxySlim.props(self, poolConfig, name, factory, tid, action.limits.timeout.duration, exe))
            logContainerCreate(tid, name, action)
            initingPool += (container -> ContainerDescriptor(name, 0, action))
            Right(())
          case None =>
            logging.error(this, s"non-executable action reached the invoker")
            Left(ContainerOperationError("non-executable action reached the invoker"))
        }
      } recoverWith {
        case t =>
          val msg = t match {
            case _: NoDocumentException           => Messages.actionRemovedWhileInvoking
            case _: DocumentTypeMismatchException => Messages.actionMismatchWhileInvoking
            case _                                => Messages.actionFetchErrorWhileInvoking
          }
          Success(Left(ContainerOperationError(msg)))
      } foreach { r =>
        origin ! r
      }

    case ContainerProxySlim.InitResult(result) =>
      val ContainerDescriptor(name, _, action) = initingPool(sender())
      initingPool -= sender()

      result match {
        case Right(_) =>
          val cap = action.limits.concurrency.maxConcurrent
          logging.info(this, s"container $name initialized, capacity is $cap")
          freePool += (sender() -> ContainerDescriptor(name, cap, action))
          messageBroker ! MessageBroker.NeedMessage(cap)
        case Left(_) =>
          logging.error(this, s"container $name failed to initialize")
      }

    case ContainerProxySlim.Done(msg, result) =>
      val tid = msg.transid
      val uuid = msg.user.namespace.uuid

      ack(tid, result, msg.blocking, msg.rootControllerIndex, msg.user.namespace.uuid, false) onComplete { _ =>
        ack(tid, result, msg.blocking, msg.rootControllerIndex, uuid, true)
        storeActivation(msg, result)
      }
      if (freePool.contains(sender())) {
        val d @ ContainerDescriptor(_, cap, _) = freePool(sender())
        freePool += (sender() -> d.changeCapacity(cap + 1))
      } else {
        val d = busyPool(sender())
        busyPool -= sender()
        freePool += (sender() -> d.changeCapacity(1))
      }

      messageBroker ! MessageBroker.NeedMessage(1)

    case ContainerProxySlim.Removed =>
    // TODO: impl remove
  }

  private def logContainerCreate(tid: TransactionId, name: String, action: WhiskAction): Unit = {
    val namespace = action.namespace.namespace
    val actionName = action.name.asString
    val maxConcurrent = action.limits.concurrency.maxConcurrent

    tid.mark(
      this,
      LoggingMarkers.INVOKER_CONTAINER_START("coldStart"),
      s"create container $name, action: $actionName, namespace: $namespace, concurrency: $maxConcurrent",
      akka.event.Logging.InfoLevel)
  }

  private def storeActivation(msg: ActivationMessage, result: WhiskActivation): Future[Any] = {
    implicit val tid: TransactionId = msg.transid
    val context = UserContext(msg.user)
    activationStore.storeAfterCheck(result, context)(tid, None)
  }

  private val ack: InvokerReactive.ActiveAck = (tid: TransactionId,
                                                activationResult: WhiskActivation,
                                                blockingInvoke: Boolean,
                                                controllerInstance: ControllerInstanceId,
                                                userId: UUID,
                                                isSlotFree: Boolean) => {
    implicit val transId: TransactionId = tid

    def send(res: Either[ActivationId, WhiskActivation], recovery: Boolean = false) = {
      val msg = if (isSlotFree) {
        val aid = res.fold(identity, _.activationId)
        val isWhiskSystemError = res.fold(_ => false, _.response.isWhiskError)
        CompletionMessage(transId, aid, isWhiskSystemError, instance)
      } else {
        ResultMessage(transId, res)
      }

      producer.send(topic = "completed" + controllerInstance.asString, msg).andThen {
        case Success(_) =>
          logging.info(
            this,
            s"posted ${if (recovery) "recovery" else "completion"} of activation ${activationResult.activationId}")
      }
    }

    if (UserEvents.enabled && isSlotFree) {
      EventMessage.from(activationResult, s"invoker${instance.instance}", userId) match {
        case Success(msg) => UserEvents.send(producer, msg)
        case Failure(t)   => logging.error(this, s"activation event was not sent: $t")
      }
    }

    send(Right(if (blockingInvoke) activationResult else activationResult.withoutLogsOrResult)).recoverWith {
      case t if t.getCause.isInstanceOf[RecordTooLargeException] =>
        send(Left(activationResult.activationId), recovery = true)
    }
  }

  private def store(tid: TransactionId, activation: WhiskActivation, context: UserContext) = {
    implicit val transId: TransactionId = tid
    activationStore.storeAfterCheck(activation, context)(tid, notifier = None)
  }
}
