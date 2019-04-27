package org.apache.openwhisk.core.scheduler

import akka.actor.ActorRef
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}

class ActivationStage(queueActor: ActorRef) extends GraphStage[FlowShape[Int, DummyActivation]] {
  val in: Inlet[Int] = Inlet("activation-stage-in")
  val out: Outlet[DummyActivation] = Outlet("activation-stage-out")

  override def shape: FlowShape[Int, DummyActivation] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    // setup sender
    implicit def self: ActorRef = stageActor.ref

    var remainingWindow: Int = 0
    var outInDemand = false
    var buffered: Option[DummyActivation] = None

    setHandler(
      in,
      new InHandler {
        override def onPush(): Unit = {
          remainingWindow += grab(in)
          // get new window advertisement eagerly
          pull(in)
          if (outInDemand) {
            trySend()
            outInDemand = false
          }
        }

        override def onUpstreamFinish(): Unit = {
          buffered match {
            case Some(act) => emit(out, act)
            case None      =>
          }
          completeStage()
        }
      })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        if (remainingWindow > 0) {
          trySend()
        } else {
          outInDemand = true
        }
      }
    })

    override def preStart(): Unit = {
      getStageActor(messageHandler)
      queueActor ! Queue.RegisterStage
      pull(in)
    }

    private def messageHandler(receive: (ActorRef, Any)): Unit = receive match {
      case (_, Queue.Fetched(act)) =>
        if (isAvailable(out)) {
          sendToOut(act)
        } else {
          buffered = Some(act)
        }
      case _ => ??? // make match exhaustive
    }

    private def trySend(): Unit = {
      buffered match {
        case Some(act) =>
          sendToOut(act)
          buffered = None
        case None =>
      }
      queueActor ! Queue.FetchRequest
    }

    private def sendToOut(elem: DummyActivation): Unit = {
      push(out, elem)
      remainingWindow -= 1
    }
  }
}
