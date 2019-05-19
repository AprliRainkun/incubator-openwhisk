package org.apache.openwhisk.core.scheduler

final case class SchedulerConfig(host: String, port: Int, actionContainerReserve: Int, maxQueueLength: Int)
