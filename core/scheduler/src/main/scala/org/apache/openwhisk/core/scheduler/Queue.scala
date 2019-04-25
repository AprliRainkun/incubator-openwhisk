package org.apache.openwhisk.core.scheduler

import scala.collection.immutable
import akka.actor.{Actor, Props}

object Queue {
  def props(name: String) = Props(new Queue(name))
}

class Queue(name: String) extends Actor {
  import QueueManager._

  private var queue = immutable.Queue.empty[DummyActivation]

  override def receive: Receive = {
    case AppendActivation(act) =>
      queue = queue.enqueue(act)
      sender ! Succeed
  }
}
