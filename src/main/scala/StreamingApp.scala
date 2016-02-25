import akka.actor.{ Actor, Props }
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.receiver._
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Helloer extends Actor with ActorHelper {
  override def preStart() = {
    println("")
    println("=== Helloer is starting up ===")
    println(s"=== path=${context.self.path} ===")
    println("")
  }
  def receive = {
    // store() method allows us to store the message so Spark Streaming knows about it
    // This is the integration point (from Akka's side) between Spark Streaming and Akka
    case s => store(s)
  }
}

object StreamingApp {
  def getMinutesDiff(a: String, b: String): Int = {
    0
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    // Configuration for a Spark application.
    // Used to set various Spark parameters as key-value pairs.
    val driverPort = 7777
    val driverHost = "localhost"
    val conf = new SparkConf(false) // skip loading external settings
      .setMaster("local[*]") // run locally with as many threads as CPUs
      .setAppName("Spark Streaming with Scala and Akka") // name in web UI
      .set("spark.logConf", "true")
      .set("spark.driver.port", driverPort.toString)
      .set("spark.driver.host", driverHost)
      .set("spark.akka.logLifecycleEvents", "true")
    val ssc = new StreamingContext(conf, Seconds(1))
    val impressionsActorName = "impressions"
    val clicksActorName = "clicks"

    // This is the integration point (from Spark's side) between Spark Streaming and Akka system
    // It's expected that the actor we're now instantiating will `store` messages (to close the integration loop)
    val impressionsActorStream = ssc.actorStream[String](Props[Helloer], impressionsActorName)
    val clicksActorStream = ssc.actorStream[String](Props[Helloer], clicksActorName)

    // describe the computation on the input stream as a series of higher-level transformations
    // actorStream.reduce(_ + " " + _).print()

    val impressionData = impressionsActorStream.window(Seconds(60), Seconds(3))
    // impressions.foreachRDD((rdd: RDD[String]) => {
    //   // some RDD operation for each window
    // })
    val clickData = clicksActorStream.window(Seconds(60), Seconds(3))
    // localActor ! s"$id_impression,$country,$campaign,$advertiser,$publisher,$timestamp"

    val impressions = impressionData.map(x =>
      (x.split(',').lift(0).get, x.split(',').lift(1).get, x.split(',').lift(5).get))
      .map({ case (impressionId, country, timestamp) => (impressionId) -> (impressionId, country, timestamp) })

    val clicks = clickData.map(x =>
      (x.split(',').lift(1).get, x.split(',').lift(2).get, x.split(',').lift(0).get))
      .map({ case (clickId, timestamp, impressionId) => (impressionId) -> (clickId, timestamp, impressionId) })

    val joined = impressions.join(clicks).map(x => x._2)

    // ((1,2,Wed Feb 24 23:07:07 CLT 2016),(2,Wed Feb 24 23:07:01 CLT 2016,1))
    val resultByCountry = joined.map(x => (x._1.productElement(1), getMinutesDiff(x._1.productElement(2).toString, x._2.productElement(1).toString)))
      .filter(x => x._2 <= 360) // 6 hours interval filter
      .mapValues(_ => 1)
      .reduceByKey(_ + _)
    // .countByKey()

    // start the streaming context so the data can be processed
    // and the actor gets started
    // impressions.print()
    // clicks.print()
    // joined.print()
    resultByCountry.print()

    ssc.start()

    scala.io.StdIn.readLine("Press Enter to stop Spark Streaming context and the application...")
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
