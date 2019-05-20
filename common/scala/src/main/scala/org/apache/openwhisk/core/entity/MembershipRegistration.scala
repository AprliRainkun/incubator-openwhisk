package org.apache.openwhisk.core.entity

import spray.json._

case class SchedulerRegistration(instance: Int, host: String, port: Int)

object SchedulerRegistration extends DefaultJsonProtocol {
  implicit val format: RootJsonFormat[SchedulerRegistration] = jsonFormat3(SchedulerRegistration.apply)
}

case class InvokerRegistration(instance: Int, host: String, port: Int, userMemory: ByteSize)

object InvokerRegistration extends DefaultJsonProtocol {
  implicit object Format extends RootJsonFormat[InvokerRegistration] {
    override def write(obj: InvokerRegistration): JsValue = {
      JsObject(
        "instance" -> JsNumber(obj.instance),
        "host" -> JsString(obj.host),
        "port" -> JsNumber(obj.port),
        "userMemory" -> JsString(obj.userMemory.toString))
    }

    override def read(json: JsValue): InvokerRegistration = {
      json.asJsObject.getFields("instance", "host", "port", "userMemory") match {
        case Seq(JsNumber(instance), JsString(host), JsNumber(port), JsString(sizeStr)) =>
          InvokerRegistration(instance.toInt, host, port.toInt, ByteSize.fromString(sizeStr))
      }
    }
  }
}

case class QueueRegistration(host: String, port: Int)

object QueueRegistration extends DefaultJsonProtocol {
  implicit val format: RootJsonFormat[QueueRegistration] = jsonFormat2(QueueRegistration.apply)
}
