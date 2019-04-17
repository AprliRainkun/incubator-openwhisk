package org.apache.openwhisk.core.scheduler

import akka.actor.ActorSystem

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Scheduler {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("scheduler")
    val srv = new QueueServiceServer(system).run("127.0.0.1", 8800)
    Await.result(srv, Duration.Inf)
  }
}