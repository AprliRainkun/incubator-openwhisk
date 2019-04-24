package org.apache.openwhisk.core.scheduler

import akka.NotUsed
import akka.util.Timeout
import akka.actor.ActorRef
import akka.pattern.ask
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import org.apache.openwhisk.grpc._
import org.apache.openwhisk.core.database.etcd._

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.concurrent.{ExecutionContext, Future}
import QueueManager.{CreateQueue, QueueCreated}
import akka.grpc.GrpcClientSettings

class RPCEndpoint(queueManager: ActorRef)(implicit etcdClientSettings: GrpcClientSettings,
                                          schedulerConfig: SchedulerConfig,
                                          mat: Materializer,
                                          ctx: ExecutionContext)
    extends QueueService {

  private val metadataStore = QueueMetadataStore.connect(etcdClientSettings)

  override def create(in: CreateQueueRequest): Future[CreateQueueResponse] = {
    implicit val timeout: Timeout = Timeout(5 second)

    for {
      _ <- (queueManager ? CreateQueue(in.actionName)).mapTo[QueueCreated]
      _ <- metadataStore.txnWriteEndpoint(in.actionName, schedulerConfig.endpoint)
    } yield {
      CreateQueueResponse(Option(ok), schedulerConfig.endpoint)
    }
  }

  override def put(in: Activation): Future[PutActivationResponse] = ???

  override def fetch(windows: Source[WindowAdvertisement, NotUsed]): Source[FetchResponse, NotUsed] = ???

  private def ok = ResponseStatus(200, "success")
}
