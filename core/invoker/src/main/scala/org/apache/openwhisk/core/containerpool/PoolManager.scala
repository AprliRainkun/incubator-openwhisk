package org.apache.openwhisk.core.containerpool

//import akka.actor.{Actor, ActorRef, Props}
//import akka.stream.{ActorMaterializer, Materializer, OverflowStrategy}
//import akka.stream.scaladsl.{Flow, Source}
//import scala.concurrent.duration._
//
//import scala.language.postfixOps
//
//class PoolManager {}
//
//object PoolManager {
//  def establishActivationFlow()(implicit mat: Materializer) = {
//    val (send, ticks) = Source
//      .queue(10, OverflowStrategy.fail)
//      .preMaterialize()
//    val windows = ticks
//      .conflateWithSeed[Int](_ => 0)((acc, _) => acc + 1)
//      .throttle(1, 100 millis)
//
//  }
//}
