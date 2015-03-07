package com.teambytes.handy.dynamo.replication

import com.gilt.gfc.logging.Loggable
import com.gilt.gfc.util.RateLimiter
import com.teambytes.awsleader.PeriodicTask

trait ReplicationJob[A] extends PeriodicTask with Loggable {

  def rateLimiter: RateLimiter

  def pageSize: Int

  def defaultReplicationPeriodMs: Long

  def bulkBootstrapReplicator: BruteForceReplicator[A]

  override def periodMs: Long = {
    val frequency = math.max(1, {
      val globalWriteRate = rateLimiter.getMaxFreqHz
      if (globalWriteRate == 0) {
        defaultReplicationPeriodMs    // this typically means "non-production"; pick a reasonable default
      } else {
        math.round(defaultReplicationPeriodMs.toFloat * pageSize / globalWriteRate)
      }
    })
    info("Scheduling incremental replication every %s ms".format(frequency))
    frequency
  }

  override def run(): Unit = {
    try {
      if(!bulkBootstrapReplicator.isBootstrapRequired) {
        doIncrementalReplication()
      } else {
        warn("Not doing incremental replication, bootstrap required.")
      }
    } catch {
      case e: Throwable => // important to catch *everything*, or the scheduled executor will stop scheduling
        error("Exception replicating", e)
    }
  }

  protected def doIncrementalReplication(): Unit

}
