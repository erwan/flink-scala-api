package org.apache.flink

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.flink.api.serializers._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfter
import org.apache.flink.streaming.api.functions.sink.DiscardingSink

case class Event(id: Int, values: List[String])

object Event {
  implicit val eventTypeInfo: TypeInformation[Event] = deriveTypeInformation[Event]
}

class MiniClusterTest extends AnyFlatSpec with Matchers with TestUtils with BeforeAndAfterAll with BeforeAndAfter {

  var env: StreamExecutionEnvironment = _
  var source: DataStream[Event]       = _

  val flinkCluster = new MiniClusterWithClientResource(
    new MiniClusterResourceConfiguration.Builder()
      .setNumberSlotsPerTaskManager(5)
      .setNumberTaskManagers(2)
      .build
  )

  override def beforeAll(): Unit = {
    flinkCluster.before()
  }

  override def afterAll(): Unit = {
    flinkCluster.after()
  }

  it should "List with a MiniCluster" in {
    env = StreamExecutionEnvironment.getExecutionEnvironment

    source = env.fromElements(
      Event(1, List("one", "two"))
    )

    val pipeline = source
      .map { e =>
        println("Mapping through element: " + e)
        e
      }
      .addSink(new DiscardingSink())

    env.execute()
  }

}
