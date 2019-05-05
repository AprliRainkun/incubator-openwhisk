package org.apache.openwhisk.core.containerpool

import akka.stream.stage.{AsyncCallback, GraphStageLogic, GraphStageWithMaterializedValue, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}

import scala.concurrent.{Future, Promise}

class ConflatedTickerStage extends GraphStageWithMaterializedValue[SourceShape[Int], Future[TickerSendEnd]] {
  // emit a stream of nonzero value that represents the number of accumulated ticks
  // between each pull from downstream
  val out: Outlet[Int] = Outlet("conflated-out")
  val shape = SourceShape(out)

  override def createLogicAndMaterializedValue(
    inheritedAttributes: Attributes): (GraphStageLogic, Future[TickerSendEnd]) = {
    val senderPromise = Promise[TickerSendEnd]

    val logic: GraphStageLogic = new GraphStageLogic(shape) {
      var counter = 0
      var senderCompleted = false

      override def preStart(): Unit = {
        val sendCallback = getAsyncCallback[Int] { x =>
          counter += x
          if (isAvailable(out) && counter > 0) {
            pushAndReset()
          }
        }
        val completeCallback = getAsyncCallback[Unit] {_ =>
          // delay the completion until downstream pulls
          senderCompleted = true
        }
        senderPromise.success(new TickerSendEnd(sendCallback, completeCallback))
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          if (counter > 0) {
            pushAndReset()
          }
          if (senderCompleted) {
            completeStage()
          }
        }
      })

      private def pushAndReset(): Unit = {
        push(out, counter)
        counter = 0
      }
    }

    (logic, senderPromise.future)
  }
}

class TickerSendEnd(sendFn: AsyncCallback[Int], completeFn: AsyncCallback[Unit]) {
  def send(x: Int): Unit = {
    sendFn.invoke(x)
  }

  def complete(): Unit = completeFn.invoke(())
}
