package org.apache.openwhisk.core.scheduler

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Scheduler {
  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("scheduler")
    implicit val mat: Materializer = ActorMaterializer()

    val srv = new QueueServiceServer(new QueueServiceDraftImpl()).run("127.0.0.1", 8800)
    Await.result(srv, Duration.Inf)
  }
}
