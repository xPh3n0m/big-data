// spark-submit --class LogAnalysis --master yarn-client SparkScalaWordCount/target/scala-2.10/sparkscalawordcount_2.10-1.0.jar --num-executors 25 2>err

// no limit on parallel jobs, but stdout is not displayed locally: need to look into worker log
// spark-submit --class LogAnalysis --master yarn-cluster SparkScalaWordCount/target/scala-2.10/sparkscalawordcount_2.10-1.0.jar --num-executors 25 2>err





// spark-shell --master yarn-client --num-executors 25
/*

import scala.util.parsing.combinator._


object Adder extends RegexParsers {
  def expr: Parser[Int] = (
   "("~expr~"+"~expr~")" ^^ { case "("~x~"+"~y~")" => x+y }
  | number
  )

  val number: Parser[Int] = """[0-9]+""".r ^^ { _.toInt}
}

Adder.parse(Adder.expr, "(7+4)")


object LogP extends RegexParsers {
  def logline: Parser[Any] = (
    timestamp~"""INFO org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler: Application Attempt"""~ident~"is done. finalState="~ident ^^ {
       case t~_~a~_~s => Option(t, a, s)
    }
  |  "[^\n]+".r ^^ (_ => None)
  )

  val ident: Parser[String] = "[A-Za-z0-9_]+".r
  val timestamp: Parser[String] = "2015-[0-9][0-9]-[0-9][0-9] [0-9:,]+".r
}


val l1 = """2015-03-10 05:15:53,196 INFO org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler: Application Attempt appattempt_1425682538854_1748_000001 is done. finalState=FINISHED"""

val l2 = """2015-03-10 05:15:53,196 INFO org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary: appId=application_1425682538854_1748,name=decompress,user=joe,queue=default,state=FINISHED,trackingUrl=http://master1:8088/proxy/application_1425682538854_1748/jobhistory/job/job_1425682538854_1748,appMasterHost=icdatasrv019.icdatacluster2,startTime=1425960925158,finishTime=1425960946325,finalStatus=FAILED"""

val l3 = """2015-03-03 17:59:51,137 INFO org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary: appId=application_1424932957480_0341,name=Spark shell,user=bob,queue=default,state=FINISHED,trackingUrl=http://master1:8088/proxy/application_1424932957480_0341/A,appMasterHost=icdatasrv058.icdatacluster2,startTime=1425392421737,finishTime=1425401990053,finalStatus=SUCCEEDED"""

*/



import scala.util.parsing.combinator._
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils


abstract class LogLine extends java.io.Serializable
case class Foo(ts: String, appatt: String, state: String) extends LogLine
case class AppSummary(timestamp: String, app: String, name: String, user: String, state:String,
  url:String, host: String, startTime: String, endTime: String, finalStatus: String) extends LogLine
case class UnknownLine() extends LogLine
case class AppRequest(timestamp: String, user: String, ip: String, res: String, app: String) extends LogLine
case class AppStatus(t: String, app: String, old_status: String, new_status: String) extends LogLine
case class ContainerAllocRelease(t: String, user: String, op: String, tgt: String, res: String, app: String, cont: String) extends LogLine
case class ContainerStatus(t: String, app: String, old_status: String, new_status: String) extends LogLine



// Yarn Log Parser
object LogP extends RegexParsers with java.io.Serializable {
  def logline: Parser[LogLine] = (
    timestamp
        ~"INFO org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler: Application Attempt"~ident
        ~"is done. finalState="~ident ^^ {
       case t~_~a~_~s => Foo(t, a, s)
    }
  | timestamp~"INFO org.apache.hadoop.yarn.server.resourcemanager.RMAppManager$ApplicationSummary: appId="~ident
        ~",name="~identW
        ~",user="~ident
        ~",queue=default,state="~ident
        ~",trackingUrl="~url
        ~",appMasterHost="~ident
        ~".icdatacluster2,startTime="~ident
        ~",finishTime="~ident
        ~",finalStatus="~ident ^^ {
       case t~_~app~_~name~_~user~_~state~_~url~_~host~_~stime~_~etime~_~finalStatus =>
         AppSummary(t, app, name, user, state, url, host, stime, etime, finalStatus)
    }
  | timestamp
        ~"INFO org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl: "~ident
        ~"State change from"~ident
        ~"to "~ident ^^ {
       case t~_~app~_~old_status~_~new_status => AppStatus(t, app, old_status, new_status)
    }
  | timestamp
        ~"INFO org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger: USER="~ident
        ~"OPERATION="~identW
        ~"TARGET="~ident
        ~"RESULT="~ident
        ~"APPID="~ident
        ~"CONTAINERID="~ident ^^ {
       case t~_~user~_~op~_~tgt~_~res~_~app~_~cont => ContainerAllocRelease(t, user, op, tgt, res, app, cont)
    }
  )

  val ident: Parser[String] = "[A-Za-z0-9_]+".r
  val identW: Parser[String] = "[A-Za-z0-9_ ]+".r
  val timestamp: Parser[String] = "2015-[0-9][0-9]-[0-9][0-9] [0-9:,]+".r
  val url: Parser[String] = "http://[a-zA-Z0-1.]+:[0-9]+/[a-zA-Z0-9_/]+".r

}


/*
object LogAppCreation extends RegexParsers with java.io.Serializable {
  def logline: Parser[LogLine] = (
    timestamp
        ~"INFO org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger: USER="~ident
        ~"IP="~any
        ~"OPERATION=Submit Application Request    TARGET=ClientRMService  RESULT="~ident
        ~"APPID="~ident ^^ {
       case t~_~user~_~ip~_~res~_~app => AppRequest(t, user, ip, res, app)
    }
  )

  val ident: Parser[String] = "[A-Za-z0-9_]+".r
  val identW: Parser[String] = "[A-Za-z0-9_ ]+".r
  val ip: Parser[String] = "[0-9.]+".r
  val any: Parser[String] = ".+".r
  val timestamp: Parser[String] = "2015-[0-9][0-9]-[0-9][0-9] [0-9:,]+".r
  val url: Parser[String] = "http://[a-zA-Z0-1.]+:[0-9]+/[a-zA-Z0-9_/]+".r
  val blank: Parser[String] = "[\t ]+".r
}
*/

object LogAppStatus extends RegexParsers with java.io.Serializable {
  def logline: Parser[LogLine] = (
    timestamp
        ~"INFO org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl: "~ident
        ~"State change from"~ident
        ~"to "~ident ^^ {
       case t~_~app~_~old_status~_~new_status => AppStatus(t, app, old_status, new_status)
    }
  )

  val ident: Parser[String] = "[A-Za-z0-9_]+".r
  val identW: Parser[String] = "[A-Za-z0-9_ ]+".r
  val ip: Parser[String] = "[0-9.]+".r
  val any: Parser[String] = ".+".r
  val timestamp: Parser[String] = "2015-[0-9][0-9]-[0-9][0-9] [0-9:,]+".r
  val url: Parser[String] = "http://[a-zA-Z0-1.]+:[0-9]+/[a-zA-Z0-9_/]+".r
}

object LogContainerAllocRelease extends RegexParsers with java.io.Serializable {
  def logline: Parser[LogLine] = (
    timestamp
        ~"INFO org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger: USER="~ident
        ~"OPERATION="~identW
        ~"TARGET="~ident
        ~"RESULT="~ident
        ~"APPID="~ident
        ~"CONTAINERID="~ident ^^ {
       case t~_~user~_~op~_~tgt~_~res~_~app~_~cont => ContainerAllocRelease(t, user, op, tgt, res, app, cont)
    }
  )

  val ident: Parser[String] = "[A-Za-z0-9_]+".r
  val identW: Parser[String] = "[A-Za-z0-9_ ]+".r
  val ip: Parser[String] = "[0-9.]+".r
  val any: Parser[String] = ".+".r
  val timestamp: Parser[String] = "2015-[0-9][0-9]-[0-9][0-9] [0-9:,]+".r
  val url: Parser[String] = "http://[a-zA-Z0-1.]+:[0-9]+/[a-zA-Z0-9_/]+".r
}

/*
object LogContainerStatus extends RegexParsers with java.io.Serializable {
  def logline: Parser[LogLine] = (
    timestamp
        ~"INFO org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container: "~identW
        ~"transitioned from"~ident
        ~"to "~ident ^^ {
       case t~_~app~_~old_status~_~new_status => ContainerStatus(t, app, old_status, new_status)
    }
  )

  val ident: Parser[String] = "[A-Za-z0-9_]+".r
  val identW: Parser[String] = "[A-Za-z0-9_ ]+".r
  val ip: Parser[String] = "[0-9.]+".r
  val any: Parser[String] = ".+".r
  val timestamp: Parser[String] = "2015-[0-9][0-9]-[0-9][0-9] [0-9:,]+".r
  val url: Parser[String] = "http://[a-zA-Z0-1.]+:[0-9]+/[a-zA-Z0-9_/]+".r
}
*/

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object LogAnalysis {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("LogAnalysis"))
    val lines = sc.textFile("hdfs:///datasets/clusterlogs/yarn-yarn-resourcemanager-master1.log*")

    def parseFinalStatus(l: String): LogLine =
      LogP.parse(LogP.logline, l).getOrElse(UnknownLine())

    def parseLogAppStatus(l: String): LogLine =
      LogAppStatus.parse(LogAppStatus.logline, l).getOrElse(UnknownLine())

    def parseContainerAlloc(l: String): LogLine = 
      LogContainerAllocRelease.parse(LogContainerAllocRelease.logline, l).getOrElse(UnknownLine())

/*
    parseLine(l2)
*/

    def e(a: LogLine) = a match {
      case AppSummary(t, app, name, user, state, url, host, stime, etime, "FAILED") => List(Tuple2(app, LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))))
      case AppSummary(t, app, name, user, state, url, host, stime, etime, "SUCCEEDED") => List(Tuple2(app, LabeledPoint(1.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))))
      case _ => List()
    }

    def d(a: LogLine) = a match {
      case AppSummary(t, app, name, user, state, url, host, stime, etime, "FAILED") => List(Tuple2(app, (0, 0.0)))
      case AppSummary(t, app, name, user, state, url, host, stime, etime, "SUCCEEDED") => List(Tuple2(app, (0, 1.0)))
      case AppStatus(t, app, old_status, "FAILED") => List(Tuple2(app, (1, 2.0)))
      case AppStatus(t, app, old_status, "FINISHED") => List(Tuple2(app, (1, 1.0)))
      //case ContainerAllocRelease(t, user, op, tgt, res, app, cont) => List(Tuple2(app, cont))
      case _ => List()
    }

    def f(a: LogLine) = a match {
      case AppStatus(t, app, old_status, new_status) => List(Tuple2(app, 1))
      case _ => List()
    }

    def g(a: LogLine) = a match {
      case ContainerAllocRelease(t, user, op, tgt, res, app, cont) => List(Tuple2(app, cont))
      case _ => List()
    }

    // Prepares vectors
    def q(a: List[(Int, Double)]): LabeledPoint = a match {
      case (0, x) :: xs => LabeledPoint(x, Vectors.dense(xs.map(_._2).toArray))
      case xs => LabeledPoint(0.0, Vectors.dense(xs.map(_._2).toArray))
    }


    val finalLogs = lines.map(l => parseFinalStatus(l)).flatMap(d).groupBy(_._1).mapValues( _.map( _._2 ) )
    val parsedLogs = finalLogs.mapValues(_.toList.sortWith( (x,y) => x._1 < y._1 )).mapValues(q).map(_._2)

    val splits = parsedLogs.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set. 
    val scoreAndLabels = test.map { point =>
         val score = model.predict(point.features)
                     (score, point.label)
    }

    scoreAndLabels.map{case(v,p) => println("score and labels = "  +v + ' ' + p)}.collect()


    //parsedLogs.saveAsTextFile("app_final")

    //val appStatus = lines.map(l => parseLogAppStatus(l)).flatMap(f).groupBy(_._1)
    /*
    def h(a: Tuple2) = a match {
      case Tuple2(app, Tuple2("NEW", "NEW_SAVING")) => Features(a, Vector(1))
      case _ => Features(a, Vector(0))
    }
    
    for(a <- appStatus) {
      val v = new Vector()
      val feat = new Features(a._1, v)
      for(stat <- a._2) {
          case ("NEW", "NEW_SAVING") => v(1)
      }
    }
    */
    //val appContainers = lines.map(l => parseContainerAlloc(l)).flatMap(g).groupBy(_._1).mapValues( _.map( _._2 ) )
    //val ll = lines.map(l => parseLine(l)).flatMap(f).cache
    //appStatus.saveAsTextFile("app_summaries")  // in the user's HDFS home directory
    //appContainers.saveAsTextFile("app_containers")

    sc.stop()
  }
}