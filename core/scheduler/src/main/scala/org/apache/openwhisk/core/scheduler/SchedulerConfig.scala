package org.apache.openwhisk.core.scheduler

import org.apache.openwhisk.core.database.etcd.QueueMetadataStoreConfig

final case class SchedulerConfig(endpoint: String,
                                 queueMetadataStoreConfig: QueueMetadataStoreConfig =
                                   QueueMetadataStoreConfig("queue/%s/creating", "queue/$s/endpoint"))
