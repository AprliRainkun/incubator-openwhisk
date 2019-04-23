package org.apache.openwhisk.core.scheduler

import akka.actor.{Actor, Props}

object Queue{
  def props(name: String) = Props(new Queue(name))
}

class Queue(name: String) extends Actor {
  override def receive: Receive = Actor.emptyBehavior
}
