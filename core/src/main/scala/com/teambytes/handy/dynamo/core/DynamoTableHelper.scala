package com.teambytes.handy.dynamo.core

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model._

import scala.collection.JavaConverters._
import scala.util.Try

object DynamoTableHelper {

  def verifyTableExists(client: AmazonDynamoDB, tableName: String, definitions: Seq[AttributeDefinition], keySchema: Seq[KeySchemaElement], localIndexes: Option[Seq[LocalSecondaryIndex]]): String = {
    val describe = client.describeTable(new DescribeTableRequest().withTableName(tableName))
    if(!definitions.toSet.equals(describe.getTable.getAttributeDefinitions.asScala.toSet)) {
      throw new ResourceInUseException("Table " + tableName + " had the wrong AttributesToGet." + " Expected: " + definitions + " " + " Was: " + describe.getTable.getAttributeDefinitions)
    } else if(!describe.getTable.getKeySchema.asScala.equals(keySchema)) {
      throw new ResourceInUseException("Table " + tableName + " had the wrong KeySchema." + " Expected: " + keySchema + " " + " Was: " + describe.getTable.getKeySchema)
    } else {
      val theirLSIs = Option(describe.getTable.getLocalSecondaryIndexes).map { localSecondaryIndexes =>
        localSecondaryIndexes.asScala.map { description =>
          new LocalSecondaryIndex().withIndexName(description.getIndexName).withKeySchema(description.getKeySchema).withProjection(description.getProjection)
        }
      }

      if(localIndexes.isDefined && localIndexes.get.toSet.equals(theirLSIs.toSet)) {
        throw new ResourceInUseException("Table " + tableName + " did not have the expected LocalSecondaryIndexes." + " Expected: " + localIndexes + " Was: " + theirLSIs)
      } else if(theirLSIs.isDefined) {
        throw new ResourceInUseException("Table " + tableName + " had local secondary indexes, but expected none." + " Indexes: " + theirLSIs)
      }

      describe.getTable.getTableStatus
    }
  }

  def verifyOrCreateTable(client: AmazonDynamoDB, tableName: String, definitions: Seq[AttributeDefinition], keySchema: Seq[KeySchemaElement], localIndexes: Option[Seq[LocalSecondaryIndex]], provisionedThroughput: ProvisionedThroughput, waitTimeSeconds: Option[Long] = None): Unit = {
    waitTimeSeconds.filter(_ > 0L).map { waitTime =>
      val status = Try {
        verifyTableExists(client, tableName, definitions, keySchema, localIndexes)
      }.recover {
        case e: ResourceNotFoundException =>
          client.createTable(new CreateTableRequest().
            withTableName(tableName).
            withAttributeDefinitions(definitions: _*).
            withKeySchema(keySchema: _*).
            withLocalSecondaryIndexes(localIndexes.map(_.asJavaCollection).orNull).
            withProvisionedThroughput(provisionedThroughput)).
            getTableDescription.getTableStatus
      }.get

      if(!TableStatus.ACTIVE.toString.equals(status)) {
        waitForTableActive(client, tableName, definitions, keySchema, localIndexes, waitTimeSeconds)
      }
    }
  }

  def waitForTableActive(client: AmazonDynamoDB, tableName: String, definitions: Seq[AttributeDefinition], keySchema: Seq[KeySchemaElement], localIndexes: Option[Seq[LocalSecondaryIndex]], waitTimeSeconds: Option[Long] = None): Unit = {
    waitTimeSeconds.filter(_ > 0L).map { waitTime =>
      val startTimeMs = System.currentTimeMillis()
      var elapsedMs = 0L
      var status: String = ""

      do {
        status = verifyTableExists(client, tableName, definitions, keySchema, localIndexes)

        if(TableStatus.DELETING.toString.equals(status)) {
          throw new ResourceInUseException("Table " + tableName + " is " + status + ", and waiting for it to become ACTIVE is not useful.")
        }

        Thread.sleep(10000L)
        elapsedMs = System.currentTimeMillis() - startTimeMs
      } while(elapsedMs / 1000.0D < waitTime || TableStatus.ACTIVE.toString.equals(status))

      if(elapsedMs / 1000.0D >= waitTime) {
        throw new ResourceInUseException("Table " + tableName + " did not become ACTIVE after " + waitTimeSeconds + " seconds.")
      }
    }

  }

}
