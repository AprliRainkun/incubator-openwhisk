package org.apache.openwhisk.core.scheduler

final case class SchedulerConfig(host: String, port: Int) {
  def endpoint: String = s"$host:$port"
}
