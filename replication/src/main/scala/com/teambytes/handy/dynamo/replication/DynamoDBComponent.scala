package com.teambytes.handy.dynamo.replication

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.github.dwhjames.awswrap.dynamodb.{AmazonDynamoDBScalaMapper, AmazonDynamoDBScalaClient, DynamoDBSerializer}

import scala.concurrent.Await
import scala.concurrent.duration._

trait DynamoDBApi[A] {
  def insertOrUpdate(obj: Seq[A])(implicit serializer: DynamoDBSerializer[A])
}

trait DynamoDBComponent[A] extends DynamoDBApi[A] {

  def sdkClient: AmazonDynamoDBAsyncClient
  protected lazy val dynamoDb = new DynamoDB(sdkClient)
  protected lazy val client = new AmazonDynamoDBScalaClient(sdkClient)
  protected lazy val mapper = AmazonDynamoDBScalaMapper(client)

  override def insertOrUpdate(objs: Seq[A])(implicit serializer: DynamoDBSerializer[A]) {
    Await.result(
      for {
        _ <- mapper.batchDump[A](objs)
        insertCount <- mapper.countScan[A]()
      } yield {
        assert {
          insertCount == objs.size
        }
      }, 20.minutes)
  }

}
