package org.apache.openwhisk.grpc

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.Materializer

import scala.concurrent.ExecutionContext

class ClientPool[C: GrpcClientUsingSettings] {
  private var list = Map.empty[(String, Int), C]

  def getClient(host: String, port: Int)(implicit sys: ActorSystem, mat: Materializer, ex: ExecutionContext): C = {
    this.synchronized {
      list.get((host, port)) match {
        case Some(client) => client
        case None =>
          val settings = GrpcClientSettings.connectToServiceAt(host, port).withTls(false)
          val client = implicitly[GrpcClientUsingSettings[C]].connect(settings)
          list += ((host, port) -> client)
          client
      }
    }
  }
}

trait GrpcClientUsingSettings[C] {
  def connect(settings: GrpcClientSettings)(implicit mat: Materializer, ex: ExecutionContext): C
}

object GrpcClientUsingSettings {
  implicit object SchedulerClient extends GrpcClientUsingSettings[QueueServiceClient] {
    override def connect(settings: GrpcClientSettings)(implicit mat: Materializer,
                                                       ex: ExecutionContext): QueueServiceClient =
      QueueServiceClient(settings)
  }

  implicit object InvokerClient extends GrpcClientUsingSettings[ContainerManagementServiceClient] {
    override def connect(settings: GrpcClientSettings)(implicit mat: Materializer,
                                                       ex: ExecutionContext): ContainerManagementServiceClient =
      ContainerManagementServiceClient(settings)
  }

}
