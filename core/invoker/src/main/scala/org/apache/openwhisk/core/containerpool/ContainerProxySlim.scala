package org.apache.openwhisk.core.containerpool

import java.time.Instant

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.pipe
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.connector.ActivationMessage
import org.apache.openwhisk.core.entity.ExecManifest.ImageName
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.http.Messages
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object ContainerProxySlim {
  def props(pool: ActorRef,
            poolConfig: ContainerPoolConfig,
            containerName: String,
            factory: (TransactionId, String, ImageName, Boolean, ByteSize, Int) => Future[Container],
            triggerTid: TransactionId,
            initTimeout: FiniteDuration,
            action: ExecutableWhiskAction)(implicit logging: Logging, instance: InvokerInstanceId): Props =
    Props(new ContainerProxySlim(pool, poolConfig, containerName, factory, triggerTid, initTimeout, action))

  // messages
  final case class InitResult(result: Either[String, Container])
  final case class Start(msg: ActivationMessage)
  final case class Done(msg: ActivationMessage, activationResult: WhiskActivation)
  final case object Removed

  // private messages
  private case class ContainerInitialized(container: Container)
}

class ContainerProxySlim(pool: ActorRef,
                         poolConfig: ContainerPoolConfig,
                         containerName: String,
                         factory: (TransactionId, String, ImageName, Boolean, ByteSize, Int) => Future[Container],
                         triggerTid: TransactionId,
                         initTimeout: FiniteDuration,
                         action: ExecutableWhiskAction)(implicit logging: Logging, instance: InvokerInstanceId)
    extends Actor {
  import ContainerProxySlim._
  implicit val ex: ExecutionContext = context.dispatcher

  factory(
    triggerTid,
    containerName,
    action.exec.image,
    action.exec.pull,
    action.limits.memory.megabytes.MB,
    poolConfig.cpuShare(action.limits.memory.megabytes.MB))
    .flatMap { container =>
      implicit val tid: TransactionId = triggerTid
      initContainer(container, action, initTimeout)
    } pipeTo self

  override def receive: Receive = {
    case r @ InitResult(inner) =>
      inner match {
        case Right(container) =>
          context.become(initialized(container))
          logging.info(this, s"container init finished, signal action pool")(TransactionId.invokerNanny)
        case Left(msg) =>
          logging.error(this, s"failed to init container, $msg")
      }
      pool ! r
  }

  private def initialized(container: Container): Receive = {
    case Start(msg) =>
      implicit val tid: TransactionId = msg.transid
      logging.info(this, s"start executing action in container")
      run(container, action, msg) map { result =>
        Done(msg, result)
      } pipeTo pool
  }

  private def initContainer(container: Container, action: ExecutableWhiskAction, timeout: FiniteDuration)(
    implicit tid: TransactionId): Future[InitResult] = {
    container
      .initialize(action.containerInitializer, timeout, action.limits.concurrency.maxConcurrent)
      .map { _ =>
        InitResult(Right(container))
      } recoverWith {
      case t =>
        logging.error(this, s"container $containerName failed to initialize, $t")
        Future.successful(InitResult(Left("failed")))
    }
  }

  private def run(container: Container, action: ExecutableWhiskAction, msg: ActivationMessage)(
    implicit tid: TransactionId): Future[WhiskActivation] = {
    val actionTimeout = action.limits.timeout.duration
    val parameters = msg.content getOrElse JsObject.empty
    val authEnvironment = {
      if (action.annotations.isTruthy(WhiskAction.provideApiKeyAnnotationName, valueForNonExistent = true)) {
        msg.user.authkey.toEnvironment
      } else JsObject.empty
    }
    val environment = JsObject(
      "namespace" -> msg.user.namespace.name.toJson,
      "action_name" -> msg.action.qualifiedNameWithLeadingSlash.toJson,
      "activation_id" -> msg.activationId.toString.toJson,
      "deadline" -> (Instant.now.toEpochMilli + actionTimeout.toMillis).toString.toJson)

    container
      .run(
        parameters,
        JsObject(authEnvironment.fields ++ environment.fields),
        actionTimeout,
        action.limits.concurrency.maxConcurrent)
      .map {
        case (runInterval, response) =>
          constructWhiskActivation(action, msg, response, runInterval, runInterval.duration >= actionTimeout)
      }
      .recover {
        case t =>
          logging.error(this, s"caught unexpected error while running activation $t")
          constructWhiskActivation(
            action,
            msg,
            ActivationResponse.whiskError(Messages.abnormalRun),
            Interval.zero,
            isTimeout = false)
      }
    // TODO: fetch container logs
  }

  private def constructWhiskActivation(action: ExecutableWhiskAction,
                                       msg: ActivationMessage,
                                       response: ActivationResponse,
                                       totalInterval: Interval,
                                       isTimeout: Boolean) = {
    val causedBy = Some {
      if (msg.causedBySequence) {
        Parameters(WhiskActivation.causedByAnnotation, JsString(Exec.SEQUENCE))
      } else {
        Parameters(
          WhiskActivation.waitTimeAnnotation,
          Interval(msg.transid.meta.start, totalInterval.start).duration.toMillis.toJson)
      }
    }
    val initTime = Parameters(WhiskActivation.initTimeAnnotation, 0.toJson)

    WhiskActivation(
      activationId = msg.activationId,
      namespace = msg.user.namespace.name.toPath,
      subject = msg.user.subject,
      cause = msg.cause,
      name = action.name,
      version = action.version,
      start = totalInterval.start,
      end = totalInterval.end,
      duration = Some(totalInterval.duration.toMillis),
      response = response,
      annotations = {
        Parameters(WhiskActivation.limitsAnnotation, action.limits.toJson) ++
          Parameters(WhiskActivation.pathAnnotation, JsString(action.fullyQualifiedName(false).asString)) ++
          Parameters(WhiskActivation.kindAnnotation, JsString(action.exec.kind)) ++
          Parameters(WhiskActivation.timeoutAnnotation, JsBoolean(isTimeout)) ++
          causedBy ++ initTime
      })
  }
}
