package com.teambytes.handy.dynamo.replication

import java.util.concurrent.{TimeUnit, Executors, ScheduledExecutorService, ScheduledFuture}

import com.gilt.gfc.logging.Loggable
import com.teambytes.awsleader.{PeriodicTask, LeaderActionsHandler}

trait ReplicationLeaderElectionHandler[A] extends LeaderActionsHandler with Loggable {

  private val interruptTaskThreads = false

  // future around the periodic task, if we are leader & replicating
  @volatile private var scheduledFutures: Iterable[ScheduledFuture[_]] = Iterable.empty
  @volatile private var isLeader = false

  def replicationJob: ReplicationJob[A]

  def bulkBootstrapReplicator: BruteForceReplicator[A]

  def tasks: Set[PeriodicTask]

  private val leaderExecutor: ScheduledExecutorService = Executors.newScheduledThreadPool(1)

  private val allTasks = Set(replicationJob) ++ tasks

  override def onIsLeader(): Unit = {
    isLeader = true
    // Only schedule tasks if we're not already the leader
    if (scheduledFutures.isEmpty) {
      if(bulkBootstrapReplicator.isBootstrapRequired) {
        bulkBootstrapReplicator.bootstrapDynamoDb(callback = {
          if(isLeader) {
            info("Starting leader tasks")
            scheduledFutures = allTasks.map(task => leaderExecutor.scheduleAtFixedRate(task, task.initialDelayMs, task.periodMs, TimeUnit.MILLISECONDS))
          } else {
            warn("Not starting leader tasks after bulk load callback as we've lost the leadership.")
          }
        })
      }
    }
  }

  override def onIsNotLeader(): Unit = {
    isLeader = false
    info("Stopping leader tasks")
    scheduledFutures.map(_.cancel(interruptTaskThreads))
    scheduledFutures = Iterable.empty
  }

}
