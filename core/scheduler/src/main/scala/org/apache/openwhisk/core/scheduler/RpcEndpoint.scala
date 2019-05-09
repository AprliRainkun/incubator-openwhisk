package org.apache.openwhisk.core.scheduler

import akka.NotUsed
import akka.actor.ActorRef
import akka.grpc.GrpcClientSettings
import akka.pattern.ask
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import akka.util.Timeout
import org.apache.openwhisk.core.database.etcd._
import org.apache.openwhisk.core.scheduler.QueueManager._
import org.apache.openwhisk.grpc._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

class RpcEndpoint(queueManager: ActorRef)(implicit etcdClientSettings: GrpcClientSettings,
                                          schedulerConfig: SchedulerConfig,
                                          mat: Materializer,
                                          ctx: ExecutionContext)
    extends QueueService {

  private val metadataStore = QueueMetadataStore.connect(schedulerConfig.queueMetadataStoreConfig, etcdClientSettings)

  override def create(in: CreateQueueRequest): Future[CreateQueueResponse] = {
    implicit val timeout: Timeout = Timeout(5 second)

    for {
      _ <- (queueManager ? CreateQueue(in.actionName)).mapTo[QueueCreated]
      _ <- metadataStore.txnWriteEndpoint(in.actionName, schedulerConfig.endpoint)
    } yield {
      CreateQueueResponse(Some(ok), schedulerConfig.endpoint)
    }
  }

  override def put(in: Activation): Future[PutActivationResponse] = {
    implicit val timeout: Timeout = Timeout(5 seconds)

    (queueManager ? AppendActivation(in)).mapTo[AppendResult] map {
      case Right(_)            => ok
      case Left(QueueNotExist) => notFound
      case Left(Overloaded)    => ResponseStatus(429, "queue too long, try later")
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

  private def ok = ResponseStatus(200, "success")

  private def notFound = ResponseStatus(400, "queue not found")
}
