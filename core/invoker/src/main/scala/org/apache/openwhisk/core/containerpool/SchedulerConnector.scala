package org.apache.openwhisk.core.containerpool

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.Materializer
import org.apache.openwhisk.grpc._

import scala.concurrent.ExecutionContext

object SchedulerConnector {
  private var list = Map.empty[(String, Int), QueueServiceClient]

  def getClient(host: String,
                port: Int)(implicit sys: ActorSystem, ex: ExecutionContext, mat: Materializer): QueueServiceClient = {
    this.synchronized {
      list.get((host, port)) match {
        case Some(client) => client
        case None =>
          val settings = GrpcClientSettings.connectToServiceAt(host, port).withTls(false)
          val client = QueueServiceClient(settings)
          list += ((host, port) -> client)
          client
      }
    }
  }
}
