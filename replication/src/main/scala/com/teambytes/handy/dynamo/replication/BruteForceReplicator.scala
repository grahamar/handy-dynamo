package com.teambytes.handy.dynamo.replication

import java.util
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ArrayBlockingQueue, Executors, TimeUnit}

import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec
import com.amazonaws.services.dynamodbv2.document.{Item, Table}
import com.gilt.gfc.logging.Loggable
import com.gilt.gfc.time.{Timer, Timestamp}
import com.gilt.gfc.util.RateLimiter
import com.github.dwhjames.awswrap.dynamodb.DynamoDBSerializer
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTimeZone, DateTime}

import scala.annotation.tailrec

/**
 * The BruteForce replicator runs when the dynamo database is empty,
 * to copy records from a relational DB to dynamo as quickly as possible.
 *
 * This only gets run if there is no cutoff date in Dynamo.
 */
object BruteForceReplicator {
  // If you delete this record, the entire cluster will restart and start building the dynamo db.
  // So don't do that.  Obviously if you drop the table that will happen as well.
  val CutoffKey = "CutoffKeyDONOTDELETE"
  val BootstrapLock = "BootstrapLock"
  val BatchSize = 10000
  val Id = "guid"
}

trait BruteForceReplicator[A] extends SqlDbApi[A] with DynamoDBComponent[A] with Loggable {

  def rateLimiter: RateLimiter

  def pageSize: Int

  def numThreads: Int

  def table: Table

  def bootstrapDynamoDb(callback: => Unit = () => ())(implicit serializer: DynamoDBSerializer[A]) {
    if (isBootstrapRequired) {
      bruteForceReplicate(callback)
    }
  }

  /**
   * returns true if this instance should do a brute force replication.
   * otherwise will block until the replication is done, and return false
   */
  def isBootstrapRequired: Boolean = {
    @tailrec
    def recurse(): Boolean = {
      val cutoff = getLastCutoff
      if (cutoff.isDefined) {
        info("Database appears to have been bootstrapped already; not kicking off bootstrap")
        false
      } else {
        val bootstrapStarted: Option[Item] =
          Option(table.getItem(new GetItemSpec()
            .withPrimaryKey(BruteForceReplicator.Id, BruteForceReplicator.BootstrapLock)
            .withConsistentRead(true)))
        if (bootstrapStarted.isDefined) {
          warn("Bootstrap started by some other process; waiting for it to complete")
          Thread.sleep(60 * 1000)
          recurse()
        } else {
          val now = new DateTime(DateTimeZone.UTC)
          val uuid = UUID.randomUUID
          val exists: Option[Item] = Option(table.putItem(new Item()
            .withPrimaryKey(BruteForceReplicator.Id, BruteForceReplicator.BootstrapLock)
            .withString("timestamp", ISODateTimeFormat.basicDateTime().print(now))
            .withString("uuid", uuid.toString)).getItem)
          if (exists == None || exists.get.getString("uuid") != uuid.toString) {
            Runtime.getRuntime.addShutdownHook(new Thread {
              override def run() {
                info("Deleting bootstrap lock")
                finishBootstrap()
              }
            })
            true
          } else {
            warn("Bootstrap (%s) just started on another server; blocking for 1 minute and trying again".format(exists.get))
            Thread.sleep(60 * 1000)
            recurse()
          }
        }
      }
    }
    recurse()
  }

  private def bruteForceReplicate(callback: => Unit = () => ())(implicit serializer: DynamoDBSerializer[A]) {
    try {
      // We manage 2 executors: one for reading from postgres, and one for writing to dynamo.
      info("Starting brute force replication with %d threads".format(numThreads))
      val coreExecutor = Executors.newFixedThreadPool(numThreads)

      val end = new Timestamp(System.currentTimeMillis)

      info("Computing which records to replicate")
      val recordCount = countDbObjects()
      val pageCount = ((recordCount / pageSize) + (if ((recordCount % pageSize) > 0) 1 else 0)).toInt
      if (pageCount > 0) {
        info("Starting job: replicate %s records (%d pages of %d)".format(recordCount, pageCount, pageSize))
      } else {
        warn("Brute force replication with nothing to replicate. This is bad.")
      }

      val pageCounter = new AtomicInteger(pageCount)

      val queueSize = math.max(numThreads, 10000 / pageSize)
      val queue = new ArrayBlockingQueue[Iterable[A]](queueSize)

      // count down pages
      val firstWrite = System.currentTimeMillis

      // Create a bunch of futures, each one of which reads a page's worth
      // of data from the db. Note this uses the dbExecutor,
      // so we can vary the number of read threads independent of the number
      // of write threads
      for (page <- 0 until pageCount) {
        coreExecutor.submit {
          new Runnable {
            override def run() {
              var (startTime, endTime) = (0l, 0l)
              try {
                startTime = System.currentTimeMillis
                queue.put(loadDbObjects(page, pageSize))
                endTime = System.currentTimeMillis
              } catch {
                case e: Throwable =>
                  error("Could not read page %s from database".format(page), e)
                  throw e
              } finally {
                // some indication of how long to go if we are doing a big job
                val now = System.currentTimeMillis()
                val thisPage = pageCount - pageCounter.decrementAndGet()
                if (thisPage == pageCount) {
                  queue.put(Nil)
                }
                val percent = 100d * thisPage / pageCount
                val pagesPerMs = 1d * thisPage / (now - firstWrite)
                val pagesLeft = pageCount - thisPage
                val msLeft = pagesLeft / pagesPerMs
                val timeLeft = Timer.pretty(TimeUnit.MILLISECONDS.toNanos(math.round(msLeft)))
                val timeSoFar = Timer.pretty(TimeUnit.MILLISECONDS.toNanos(now - firstWrite))
                val hz = 1000d * pageSize / (endTime - startTime)
                val pseudoHz = hz * numThreads
                info("Replicated (%s/%s) %3.1f%% (%3.2f Hz) {%3.2f Hz} (Finish in %s) (So far: %s)".format(
                  thisPage, pageCount, percent, hz, pseudoHz, timeLeft, timeSoFar))
              }
            }
          }
        }
      }

      val drain = new util.ArrayList[Iterable[A]](pageSize * numThreads)
      var done = pageCount == 0
      import scala.collection.JavaConverters._
      while (!done) {
        drain.clear()
        queue.drainTo(drain)
        val toDrain = drain.asScala
        done = toDrain.contains(Nil)
        toDrain.flatten.grouped(BruteForceReplicator.BatchSize).foreach(objs => rateLimiter.limit(insertOrUpdate(objs)))
      }
      info("Records loaded into dynamo")
      setLastCutoff(end) // if we made it this far, set the cutoff for next time
      callback
    } finally {
      finishBootstrap()
    }
  }

  private def finishBootstrap() {
    table.deleteItem(BruteForceReplicator.Id, BruteForceReplicator.BootstrapLock)
  }

  // if we've already migrated some entries, they were all modified <= this timestamp
  private def getLastCutoff: Option[Timestamp] = {
    val date: Option[DateTime] = Option(table.getItem(new GetItemSpec()
      .withPrimaryKey(BruteForceReplicator.Id, BruteForceReplicator.CutoffKey)
      .withConsistentRead(true))).map { x =>
        ISODateTimeFormat.basicDateTime().parseDateTime(x.getString("timestamp"))
      }
    date.map(d => new Timestamp(d.getMillis))
  }

  // when we're done, store the timestamp for entries modified <= it
  private def setLastCutoff(timestamp: Timestamp) {
    table.putItem(new Item()
      .withPrimaryKey(BruteForceReplicator.Id, BruteForceReplicator.CutoffKey)
      .withString("timestamp", ISODateTimeFormat.basicDateTime().print(timestamp.getTime)))
  }
}
