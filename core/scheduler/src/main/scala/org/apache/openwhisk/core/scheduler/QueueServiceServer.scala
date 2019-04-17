package org.apache.openwhisk.core.scheduler

import akka.actor.ActorSystem
import akka.http.scaladsl.UseHttp2.Always
import akka.http.scaladsl.{Http, HttpConnectionContext}
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.openwhisk.grpc.QueueServiceHandler

import scala.concurrent.{ExecutionContext, Future}

class QueueServiceServer(system: ActorSystem) {
  implicit val sys: ActorSystem = system
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = sys.dispatcher

  def run(listen: String, port: Int): Future[Unit] = {
    // create service handler
    val service =
      QueueServiceHandler(new QueueServiceDraftImpl())

    Http()
      .bindAndHandleAsync(service, listen, port, HttpConnectionContext(http2 = Always))
      .map(b => {
        println(s"gRPC server bound to ${b.localAddress}")
      })
  }
}
