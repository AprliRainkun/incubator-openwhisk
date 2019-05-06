package org.apache.openwhisk.core.containerpool

import java.time.Instant

import akka.NotUsed
import akka.actor.{Actor, ActorSystem, Props}
import akka.event.Logging._
import akka.pattern.ask
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.Timeout
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.openwhisk.common._
import org.apache.openwhisk.common.tracing.WhiskTracerProvider
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.database._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.invoker.InvokerReactive
import org.apache.openwhisk.http.Messages
import spray.json.JsString

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success}

object ContainerPoolForAction {
  def props(runMessages: Source[String, NotUsed],
            entityStore: ArtifactStore[WhiskEntity],
            activationStore: ActivationStore,
            producer: MessageProducer)(implicit instance: InvokerInstanceId, logging: Logging) =
    Props(new ContainerPoolForAction(runMessages, entityStore, activationStore, producer))

  final case object NeedMessage
}

class ContainerPoolForAction(runMessages: Source[String, NotUsed],
                             entityStore: ArtifactStore[WhiskEntity],
                             activationStore: ActivationStore,
                             producer: MessageProducer)(implicit instance: InvokerInstanceId, logging: Logging)
    extends Actor {

  implicit val sys: ActorSystem = context.system
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ex: ExecutionContext = sys.dispatcher

  implicit val timeout: Timeout = Timeout(1 second)

  runMessages
  // parallel fetch
    .mapAsyncUnordered(5) { msg =>
      convertToRunnable(msg).flatMap{
        case Some(run) => self ? run
        case None => Future.successful()
      }
    }
    .runWith(Sink.ignore)

  override def receive: Receive = {
    case r: Run =>
    // parse data
    // construct a runnable action by fetching code from database
    // ack immediately on failure
    // dispatch runnable action to container proxy

  }

  private def convertToRunnable(msg: String): Future[Option[Run]] = {
    Future(ActivationMessage.parse(msg))
      .flatMap(Future.fromTry)
      .flatMap { msg =>
        implicit val tid: TransactionId = msg.transid

        WhiskTracerProvider.tracer.setTraceContext(tid, msg.traceContext)

        val _ = tid.started(this, LoggingMarkers.INVOKER_ACTIVATION, logLevel = InfoLevel)
        val namespace = msg.action.path
        val name = msg.action.name
        val actionId = FullyQualifiedEntityName(namespace, name).toDocId.asDocInfo(msg.revision)
        val subject = msg.user.subject

        logging.debug(this, s"${actionId.id} $subject ${msg.activationId}")

        // caching is enabled since actions have revision id and an updated
        // action will not hit in the cache due to change in the revision id;
        // if the doc revision is missing, then bypass cache
        if (actionId.rev == DocRevision.empty) logging.warn(this, s"revision was not provided for ${actionId.id}")

        WhiskAction
          .get(entityStore, actionId.id, actionId.rev, fromCache = actionId.rev != DocRevision.empty)
          .flatMap { action =>
            action.toExecutableWhiskAction match {
              case Some(exe) => Future.successful(Some(Run(exe, msg)))
              case None =>
                logging.error(this, s"non-executable action reached the invoker ${action.fullyQualifiedName(false)}")
                Future.failed(new IllegalStateException("non-executable action reached the invoker"))
            }
          }
          .recoverWith {
            case t =>
              val response = t match {
                case _: NoDocumentException =>
                  ActivationResponse.applicationError(Messages.actionRemovedWhileInvoking)
                case _: DocumentTypeMismatchException | _: DocumentUnreadable =>
                  ActivationResponse.whiskError(Messages.actionMismatchWhileInvoking)
                case _ =>
                  ActivationResponse.whiskError(Messages.actionFetchErrorWhileInvoking)
              }

              val context = UserContext(msg.user)
              val activation = generateFallbackActivation(msg, response)

              // ack and store in parallel
              val ackFut =
                ack(msg.transid, activation, msg.blocking, msg.rootControllerIndex, msg.user.namespace.uuid, true)
              for {
                _ <- store(msg.transid, activation, context)
                _ <- ackFut
              } yield None
          }
      }
  }

  /** Generates an activation with zero runtime. Usually used for error cases */
  private def generateFallbackActivation(msg: ActivationMessage, response: ActivationResponse): WhiskActivation = {
    val now = Instant.now
    val causedBy = if (msg.causedBySequence) {
      Some(Parameters(WhiskActivation.causedByAnnotation, JsString(Exec.SEQUENCE)))
    } else None

    WhiskActivation(
      activationId = msg.activationId,
      namespace = msg.user.namespace.name.toPath,
      subject = msg.user.subject,
      cause = msg.cause,
      name = msg.action.name,
      version = msg.action.version.getOrElse(SemVer()),
      start = now,
      end = now,
      duration = Some(0),
      response = response,
      annotations = {
        Parameters(WhiskActivation.pathAnnotation, JsString(msg.action.asString)) ++ causedBy
      })
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
