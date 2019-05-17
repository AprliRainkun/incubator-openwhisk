package org.apache.openwhisk.core.containerpool

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}

class SideChannelBackpressureStage[T](sender: TickerSendEnd) extends GraphStage[FlowShape[T, T]] {
  val in: Inlet[T] = Inlet("batch-fetch-in")
  val out: Outlet[T] = Outlet("batch-fetch-out")
  val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        push(out, grab(in))
      }
    })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        pull(in)
        // set back pressure signal through side channel
        sender.send(1)
      }

      override def onDownstreamFinish(): Unit = {
        sender.complete()
        super.onDownstreamFinish()
      }
    })
  }
}
