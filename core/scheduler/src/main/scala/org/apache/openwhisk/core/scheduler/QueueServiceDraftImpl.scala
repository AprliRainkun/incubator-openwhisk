package org.apache.openwhisk.core.scheduler

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import org.apache.openwhisk.grpc._

import scala.concurrent.Future

class QueueServiceDraftImpl(implicit mat: Materializer) extends QueueService {

  override def fetch(windows: Source[WindowAdvertisement, NotUsed]): Source[FetchActivationResponse, NotUsed] = {
    windows.map(w => {
      val activation = Activation("empty")
      FetchActivationResponse(Some(ok), Some(activation))
    })
  }

  override def create(in: CreateQueueRequest): Future[CreateQueueResponse] = {
    println(s"create request received for action ${in.actionName}")
    Future.successful(CreateQueueResponse(Some(ok), "this.cluster"))
  }

  override def put(in: Activation): Future[PutActivationResponse] = {
    println(s"put request received for action ${in.actionName}")
    Future.successful(PutActivationResponse(Some(ok)))
  }

  private def ok = ResponseStatus(200, "success")

}
