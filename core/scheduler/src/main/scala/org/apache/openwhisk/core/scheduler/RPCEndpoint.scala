package org.apache.openwhisk.core.scheduler

import akka.NotUsed
import akka.util.Timeout
import akka.actor.ActorRef
import akka.pattern.ask
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import org.apache.openwhisk.grpc._

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.concurrent.{ExecutionContext, Future}
import QueueManager.{CreateQueue, QueueCreated}

class RPCEndpoint(queueManager: ActorRef)(implicit mat: Materializer, ctx: ExecutionContext) extends QueueService {
  override def create(in: CreateQueueRequest): Future[CreateQueueResponse] = {
    implicit val timeout: Timeout = Timeout(3 second)

    queueManager ? CreateQueue(in.actionName) map { r =>
      r.asInstanceOf[QueueCreated]
    } map { created =>
      CreateQueueResponse(Option(ok), created.endpoint)
    }
  }

  override def put(in: Activation): Future[PutActivationResponse] = ???

  override def fetch(windows: Source[WindowAdvertisement, NotUsed]): Source[FetchResponse, NotUsed] = ???

  private def ok = ResponseStatus(200, "success")
}
