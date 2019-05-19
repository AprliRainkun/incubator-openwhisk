package org.apache.openwhisk.core.scheduler

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.Timeout
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.database.ArtifactStore
import org.apache.openwhisk.core.database.etcd._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.scheduler.InvokerResource._
import org.apache.openwhisk.core.scheduler.QueueManager._
import org.apache.openwhisk.grpc.{TransactionId => RpcTid, _}
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

final case class InsufficientMemoryException(msg: String) extends Exception

class RpcEndpoint(queueManager: ActorRef,
                  invokerResource: ActorRef,
                  queueMetadataStore: QueueMetadataStore,
                  entityStore: ArtifactStore[WhiskEntity],
                  schedulerConfig: SchedulerConfig)(implicit sys: ActorSystem,
                                                    mat: Materializer,
                                                    ex: ExecutionContext,
                                                    logging: Logging)
    extends QueueService {

  private val invokerClientPool = new ClientPool[ContainerManagementServiceClient]

  override def create(in: CreateQueueRequest): Future[CreateQueueResponse] = {
    implicit val timeout: Timeout = Timeout(5 second)
    implicit val tid: TransactionId = parseTid(in.tid.head)
    val docInfo = parseDocInfo(in.action.head)

    val responseFuture = for {
      _ <- createInitialContainers(docInfo)
      _ <- (queueManager ? CreateQueue(docInfo)).mapTo[QueueCreated]
      registration = QueueRegistration(schedulerConfig.host, schedulerConfig.port)
      _ <- queueMetadataStore.txnWriteEndpoint(docInfo, registration)
    } yield {
      CreateQueueResponse(Some(ok), schedulerConfig.host, schedulerConfig.port)
    }

    responseFuture recoverWith {
      case t =>
        val msg = s"failed to create queue for action $docInfo, ${t.getMessage}"
        logging.error(this, msg)
        val status = ResponseStatus(500, msg)
        Future.successful(CreateQueueResponse(Some(status)))
    }
  }

  override def put(in: Activation): Future[PutActivationResponse] = {
    implicit val timeout: Timeout = Timeout(5 seconds)
    implicit val tid: TransactionId = parseTid(in.tid.head)
    val docInfo = parseDocInfo(in.action.head)

    (queueManager ? AppendActivation(in)).mapTo[AppendResult] map {
      case Right(_) =>
        logging.debug(this, s"Successfully appended action $docInfo")
        ok
      case Left(QueueNotExist) =>
        logging.error(this, s"Failed to append action $docInfo, queue doesn't exist yet")
        notFound
      case Left(Overloaded(limit)) =>
        logging.warn(this, s"Refuse to accept action $docInfo, queue is too long, limit: $limit")
        ResponseStatus(429, s"queue too long, limit is $limit, try later")
    } map (r => PutActivationResponse(Some(r)))
  }

  override def fetch(windows: Source[WindowAdvertisement, NotUsed]): Source[FetchActivationResponse, NotUsed] = {
    implicit val timeout: Timeout = Timeout(5 seconds)

    val fut = (queueManager ? EstablishFetchStream(windows)).mapTo[EstablishResult] map {
      case Right(FetchStream(acts)) =>
        acts.map { act =>
          FetchActivationResponse(Some(ok), Some(act))
        }
      case Left(QueueNotExist) => Source(List(FetchActivationResponse(Some(notFound))))
      case _                   => ???
    }
    Source.fromFuture(fut) flatMapConcat identity
  }

  private def createInitialContainers(actionInfo: DocInfo)(implicit tid: TransactionId): Future[Unit] = {
    if (schedulerConfig.actionContainerReserve > 0) {
      implicit val timeout: Timeout = Timeout(5 second)
      for {
        action <- WhiskAction.get(
          entityStore,
          actionInfo.id,
          actionInfo.rev,
          fromCache = actionInfo.rev != DocRevision.empty)
        actionMem = ByteSize.fromString(s"${action.limits.memory.megabytes}MB")
        initContainers <- (invokerResource ? ReserveMemory(tid, actionMem, schedulerConfig.actionContainerReserve))
          .mapTo[ReserveMemoryResult]
          .map(_.result) flatMap {
          case Right(allocations) => Future.successful(allocations)
          case Left(msg)          => Future.failed(InsufficientMemoryException(msg))
        }
        _ <- allocateContainerOn(actionInfo, initContainers)
      } yield ()
    } else {
      Future.successful(())
    }
  }

  private def allocateContainerOn(action: DocInfo, allocation: Seq[Reservation])(
    implicit tid: TransactionId): Future[Unit] = {
    Future
      .traverse(allocation) {
        case Reservation(invoker, number) =>
          val client = invokerClientPool.getClient(invoker.host, invoker.port)
          val rpcTid = RpcTid(tid.toJson.compactPrint)
          val actionId = ActionIdentifier(action.id.asString, action.rev.asString)
          val req = AllocateRequest(Some(rpcTid), Some(actionId), number)

          client.allocate(req) flatMap { resp =>
            if (resp.status.head.statusCode == 200) {
              logging.info(
                this,
                s"Successfully allocate $number container for action $action on invoker ${invoker.instance}")
              Future.successful(())
            } else {
              val msg =
                s"Failed to allocate $number container for action $action on invoker ${invoker.instance}, message: ${resp.status.head.message}"
              logging.error(this, msg)
              Future.failed(new Exception(msg))
            }
          }
      }
      .map(_ => ())
  }

  private def parseDocInfo(action: ActionIdentifier) = DocInfo ! (action.id, action.revision)

  private def parseTid(rpcTid: RpcTid): TransactionId = rpcTid.raw.parseJson.convertTo[TransactionId]

  private def ok = ResponseStatus(200, "success")

  private def notFound = ResponseStatus(400, "queue not found")
}
