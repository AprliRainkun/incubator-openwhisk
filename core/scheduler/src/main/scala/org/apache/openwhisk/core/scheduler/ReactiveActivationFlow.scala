package org.apache.openwhisk.core.scheduler

import java.time._

import akka.NotUsed
import akka.pattern.ask
import akka.stream._
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.Timeout

object ReactiveActivationFlow {
  def create(handle: Queue.Handle): Flow[Int, DummyActivation, NotUsed] = {
    implicit val to: Timeout = Timeout.create(Duration.ofSeconds(10))

    val fetchFlow: Flow[Int, List[DummyActivation], NotUsed] = Flow[Int].mapAsync(1) { n =>
      (handle.queue ? Queue.RequestAtMost(handle.key, n)).mapTo[Queue.Response]
    } map (_.as)

    val flatFlow = Flow[List[DummyActivation]].mapConcat[DummyActivation](identity)

    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val broadcast = b.add(Broadcast[List[DummyActivation]](2))
      val repeater = b.add(new RepeatUntilAccumulateToStage(handle))
      val fetcher = b.add(fetchFlow)
      val flatter = b.add(flatFlow)

      // format: off
      repeater.out ~>                 fetcher                 ~> broadcast ~> flatter
      repeater.in1 <~ Flow[List[DummyActivation]].map(_.size) <~ broadcast
      // format: on
      FlowShape(repeater.in0, flatter.out)
    })
  }
}

class RepeatUntilAccumulateToStage(handle: Queue.Handle) extends GraphStage[FanInShape2[Int, Int, Int]] {
  // nonzero number
  val inTarget: Inlet[Int] = Inlet("target-source-in")
  // could be zero
  val inFeedback: Inlet[Int] = Inlet("feedback-in")
  // nonzero number
  val out: Outlet[Int] = Outlet("round-out")

  override def shape: FanInShape2[Int, Int, Int] = new FanInShape2(inTarget, inFeedback, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    var remaining = 0
    var pendingFeedback = false

    override def preStart(): Unit = {
      pull(inTarget)
      pull(inFeedback)
    }

    setHandler(
      inTarget,
      new InHandler {
        override def onPush(): Unit = {
          remaining = grab(inTarget)
          // println(s"key: ${handle.key} inTarget:onPush(), remaining = $remaining")
          if (isAvailable(out)) {
            sendRemaining()
          }
        }

        override def onUpstreamFinish(): Unit = {
          // println(s"key: ${handle.key} inTarget:OnUpstreamFinish(), pendingFeedback = $pendingFeedback")
          handle.queue ! Queue.CancelFetch(handle.key)
          completeStage()
        }
      })

    setHandler(
      inFeedback,
      new InHandler {
        override def onPush(): Unit = {
          remaining -= grab(inFeedback)
          // println(s"key: ${handle.key} inFeedback:onPush(), remaining = $remaining")
          pendingFeedback = false

          pull(inFeedback)
          if (remaining == 0) {
            pull(inTarget)
          }
          if (remaining > 0 && isAvailable(out)) {
            sendRemaining()
          }
        }
      })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        if (remaining > 0 && !pendingFeedback) {
          sendRemaining()
        }
      }
    })

    private def sendRemaining(): Unit = {
      push(out, remaining)
      pendingFeedback = true
      // println(s"key: ${handle.key} push remaining = $remaining to out")
    }
  }
}
