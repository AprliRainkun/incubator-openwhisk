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
    val memory = ByteSize.fromString(in.memory)

    logging.info(this, s"start creating queue for action $docInfo, memory claim = ${memory.toMB}MB")
    val responseFuture = for {
      // order matters
      assignment <- createContainerAssignment(docInfo, memory)
      _ <- (queueManager ? CreateQueue(docInfo)).mapTo[QueueCreated]
      registration = QueueRegistration(schedulerConfig.host, schedulerConfig.port)
      _ <- queueMetadataStore.txnWriteEndpoint(docInfo, registration)
      _ <- allocateContainerOn(docInfo, assignment)
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
        logging.info(this, s"Successfully appended action $docInfo")
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
    logging.info(this, "establishing fetch stream request received")
    val fut = (queueManager ? EstablishFetchStream(windows)).mapTo[EstablishResult] map {
      case Right(FetchStream(acts)) =>
        logging.info(this, "fetch stream established")
        acts.map { act =>
          FetchActivationResponse(Some(ok), Some(act))
        }
      case Left(QueueNotExist) =>
        logging.error(this, s"queue not found")
        Source(List(FetchActivationResponse(Some(notFound))))
      case _ => ???
    }
    Source.fromFuture(fut) flatMapConcat identity
  }

  private def createContainerAssignment(actionInfo: DocInfo, memory: ByteSize)(
    implicit tid: TransactionId): Future[Seq[Reservation]] = {
    if (schedulerConfig.actionContainerReserve > 0) {
      implicit val timeout: Timeout = Timeout(5 second)

      (invokerResource ? ReserveMemory(tid, memory, schedulerConfig.actionContainerReserve))
        .mapTo[ReserveMemoryResult]
        .map(_.result) flatMap {
        case Right(allocations) => Future successful allocations
        case Left(msg)          => Future failed InsufficientMemoryException(msg)
      }
    } else {
      Future successful Seq.empty
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
