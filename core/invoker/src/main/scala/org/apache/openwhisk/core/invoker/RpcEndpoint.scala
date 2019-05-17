package org.apache.openwhisk.core.invoker

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.UseHttp2.Always
import akka.http.scaladsl.{Http, HttpConnectionContext}
import akka.pattern.ask
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.Timeout
import org.apache.openwhisk.common
import org.apache.openwhisk.core.containerpool._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.grpc._
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

class RpcEndpoint(poolManager: ActorRef)(implicit mat: Materializer, ex: ExecutionContext)
    extends ContainerManagementService {
  override def allocate(in: AllocateRequest): Future[AllocateResponse] = {
    implicit val timeout: Timeout = Timeout(5 seconds)

    val tid = common.TransactionId.serdes.read(in.tid.head.raw.parseJson)
    val actionId = DocInfo ! (in.action.head.id, in.action.head.revision)
    (poolManager ? PoolManager.AllocateContainer(tid, actionId)).mapTo[PoolManager.ContainerOperationResult] map {
      case Right(_)     => ResponseStatus(200, "success")
      case Left(reason) => ResponseStatus(500, reason.msg)
    } map (r => AllocateResponse(Some(r)))
  }
}

class RpcServer(impl: ContainerManagementService)(implicit sys: ActorSystem, logging: common.Logging) {
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ex: ExecutionContext = sys.dispatcher

  def run(listen: String, port: Int): Future[Unit] = {
    val service = ContainerManagementServiceHandler(impl)

    Http()
      .bindAndHandleAsync(service, listen, port, HttpConnectionContext(http2 = Always))
      .map { b =>
        logging.info(this, s"gRPC server bound to ${b.localAddress}")
      }
  }
}
