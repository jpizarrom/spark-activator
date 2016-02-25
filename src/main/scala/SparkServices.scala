import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import akka.actor.Actor
import akka.actor.ActorContext
import spray.http.MediaTypes._
import spray.routing.Directive.pimpApply
import spray.routing.HttpService
import spray.routing.RequestContext
import spray.http.HttpResponse
import spray.http.StatusCodes.OK

import java.util.Calendar
import akka.actor.{ ActorSystem, Props }
import org.apache.spark.SparkEnv

trait SparkService extends HttpService {

  val sparkConf: SparkConf = new SparkConf().setAppName("spark-spray-starter").setMaster("local")
  val sc: SparkContext = new SparkContext(sparkConf)

  // implicit val system = ActorSystem("LocalSystem")
  val system = SparkEnv.get.actorSystem
  val impressionslocalActor = system.actorOf(Props[ImpressionsLocalActor], name = "ImpressionsLocalActor")
  impressionslocalActor ! "START"
  val clickslocalActor = system.actorOf(Props[ClicksLocalActor], name = "ClicksLocalActor")
  clickslocalActor ! "START"

  val sparkRoutes =
    path("spark" / "version") {
      get {
        complete {
          HttpResponse(OK, "Spark version in this template is: " + sc.version)
        }
      }
    }
  val impressionsSparkRoutes =
    path("api" / "impression") {
      get {
        parameters('id_impression, 'country, 'campaign, 'advertiser, 'publisher) { (id_impression, country, campaign, advertiser, publisher) =>
          val timestamp = Calendar.getInstance().getTime()
          impressionslocalActor ! s"$id_impression,$country,$campaign,$advertiser,$publisher,$timestamp"
          complete {
            HttpResponse(OK, s"Sample impression response")
          }
        }
      }
    }
  val clicksSparkRoutes =
    path("api" / "click") {
      get {
        parameters('id_impression, 'click) { (id_impression, click) =>
          val timestamp = Calendar.getInstance().getTime()
          clickslocalActor ! s"$id_impression,$click,$timestamp"
          complete {
            HttpResponse(OK, s"Sample click response")
          }
        }
      }
    }

}

class SparkServices extends Actor with SparkService {
  def actorRefFactory: ActorContext = context
  def receive: Actor.Receive = runRoute(sparkRoutes ~ impressionsSparkRoutes ~ clicksSparkRoutes)
}

class ImpressionsLocalActor extends Actor {
  val driverPort = 7777
  val driverHost = "localhost"
  val actorName = "impressions"
  val url = s"akka.tcp://sparkDriver@$driverHost:$driverPort/user/Supervisor0/$actorName"
  val remote = context.actorSelection(url)

  def receive = {
    case "START" =>
      remote ! "Spray API server has started"
    case msg: String =>
      remote ! s"$msg"
  }
}

class ClicksLocalActor extends Actor {
  val driverPort = 7777
  val driverHost = "localhost"
  val actorName = "clicks"
  val url = s"akka.tcp://sparkDriver@$driverHost:$driverPort/user/Supervisor1/$actorName"
  val remote = context.actorSelection(url)

  def receive = {
    case "START" =>
      remote ! "Spray API server has started"
    case msg: String =>
      remote ! s"$msg"
  }
}