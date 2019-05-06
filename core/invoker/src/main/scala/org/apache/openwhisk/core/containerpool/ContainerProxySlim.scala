package org.apache.openwhisk.core.containerpool

import java.time.Instant

import akka.actor.{Actor, ActorRef, FSM, Props}
import akka.pattern.pipe
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.connector.ActivationMessage
import org.apache.openwhisk.core.entity.ExecManifest.ImageName
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.http.Messages
import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

object ContainerProxySlim {

  // messages
  final case object Initialized
  final case class Start(tid: TransactionId, msg: ActivationMessage)
  final case class Done(tid: TransactionId,
                        activationResult: WhiskActivation,
                        blockingInvoke: Boolean,
                        controllerInstance: ControllerInstanceId,
                        userId: UUID,
                        isSlowFree: Boolean)
  final case object Removed

  // private messages
  private case class ContainerInitialized(container: Container)
}

class ContainerProxySlim(pool: ActorRef,
                         factory: (TransactionId, String, ImageName, Boolean, ByteSize, Int) => Future[Container],
                         triggerTid: TransactionId,
                         initTimeout: FiniteDuration,
                         action: ExecutableWhiskAction)(implicit logging: Logging, instance: InvokerInstanceId)
    extends Actor {
  import ContainerProxySlim._
  implicit val ex: ExecutionContext = context.dispatcher

  factory(
    triggerTid,
    ContainerProxy.containerName(instance, action.namespace.asString, action.name.asString),
    action.exec.image,
    action.exec.pull,
    action.limits.memory.megabytes.MB)
    .flatMap { container =>
      implicit val tid: TransactionId = triggerTid
      initContainer(container, action, initTimeout) map (_ => ContainerInitialized(container))
    } pipeTo self

  override def receive: Receive = {
    case ContainerInitialized(container) =>
      context.become(initialized(container))
      pool ! Initialized
  }

  private def initialized(container: Container): Receive = {
    case Start(tid, msg) =>
      implicit val transId: TransactionId = tid
      run(container, action, msg)
  }

  private def initContainer(container: Container, action: ExecutableWhiskAction, timeout: FiniteDuration)(
    implicit tid: TransactionId): Future[Unit] = {
    container.initialize(action.containerInitializer, timeout, action.limits.concurrency.maxConcurrent).map(_ => ())
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
