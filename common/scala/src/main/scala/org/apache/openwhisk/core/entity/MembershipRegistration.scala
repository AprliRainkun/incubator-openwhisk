package org.apache.openwhisk.core.entity

import spray.json._

case class SchedulerRegistration(host: String, port: Int)

object SchedulerRegistration extends DefaultJsonProtocol {
  implicit val format: RootJsonFormat[SchedulerRegistration] = jsonFormat2(SchedulerRegistration.apply)
}

case class InvokerRegistration(host: String, port: Int, userMemory: ByteSize)

object InvokerRegistration extends DefaultJsonProtocol {
  implicit object Format extends RootJsonFormat[InvokerRegistration] {
    override def write(obj: InvokerRegistration): JsValue = {
      JsObject(
        "host" -> JsString(obj.host),
        "port" -> JsNumber(obj.port),
        "userMemory" -> JsString(obj.userMemory.toString))
    }

    override def read(json: JsValue): InvokerRegistration = {
      json.asJsObject.getFields("host", "port", "userMemory") match {
        case Seq(JsString(host), JsNumber(port), JsString(sizeStr)) =>
          InvokerRegistration(host, port.toInt, ByteSize.fromString(sizeStr))
      }
    }
  }
}

case class QueueRegistration(host: String, port: Int)

object QueueRegistration extends DefaultJsonProtocol {
  implicit val format: RootJsonFormat[QueueRegistration] = jsonFormat2(QueueRegistration.apply)
}
