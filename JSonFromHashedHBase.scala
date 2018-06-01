import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.lang.Long._
import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.NewHadoopRDD
import org.json4s.JObject
import scala.annotation.serializable
import scala.util.parsing.json.JSONObject

val MD5_LENGTH = 16

class JobWrapper(sc: SparkContext) {
  val job = Job.getInstance(sc.hadoopConfiguration)
}

@transient val job = new JobWrapper(sc)
val conf = job.job.getConfiguration
HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf))

def showConfProp(prop: String, conf : Configuration) : Any = println( prop + " = " + conf.get(prop))

def md5Hash(s:String) = {
  val ms: MessageDigest = MessageDigest.getInstance("MD5")
  ms.digest(Bytes.toBytes(s))
}

val dateFormat14 = new SimpleDateFormat("yyyyMMddHHmmss")
val dateFormat17 = new SimpleDateFormat("yyyyMMddHHmmssSSS")
val dateFormat12 = new SimpleDateFormat("yyyyMMddHHmm")
val dateFormat10 = new SimpleDateFormat("yyyyMMddHH")
val dateFormat8 = new SimpleDateFormat("yyyyMMdd")

def getDatesWithFormat(s1:String,s2:String, format: String => Date): (Date,Date) = (format(s1),format(s2))

def getDates(s1:String,s2:String) : (Date,Date) = {
  if(s1.length!=s2.length)  throw new Exception("Dates should be in the same format")
  s1.length match {
    case 8 => getDatesWithFormat(s1,s2,dateFormat8.parse)
    case 10 => getDatesWithFormat(s1,s2,dateFormat10.parse)
    case 12 => getDatesWithFormat(s1,s2,dateFormat12.parse)
    case 14 => getDatesWithFormat(s1,s2,dateFormat14.parse)
    case _ => throw new Exception("wrong format: must be yyyyMMdd, yyyyMMddHH, yyyyMMddHHmm or yyyyMMddHHmmss")
  }
}

val (from,to) = getDates("yyyyMMdd","yyyyMMdd")

HBaseAdmin.checkHBaseAvailable(conf)

/**
* We want to generate a rowkey with a hashed prefix for optimal HBase spread
*/
def generateKey(id: String, ts: Date, reverse: Boolean) : Array[Byte] = {
  val reverseInt: Int = if (reverse) -1 else 1
  val l: Long = if (ts != null) ts.getTime else 0l
  val longLength: Int = SIZE / 8
  val idHash: Array[Byte] = md5Hash(id)
  val timestamp: Array[Byte] = Bytes.toBytes(reverseInt * l)
  val rowKeyLocal: Array[Byte] = new Array[Byte](MD5_LENGTH + longLength)
  var offset: Int = 0
  offset = Bytes.putBytes(rowKeyLocal, offset, idHash, 0, idHash.length)
  if (l > 0l) Bytes.putBytes(rowKeyLocal, offset, timestamp, 0, timestamp.length)
  rowKeyLocal
}

val scan = new Scan()
scan.setStartRow(generateKey("HASHED_PREFIX",from,false))
scan.setStopRow(generateKey("HASHED_PREFIX",to,false))
scan.addFamily(Bytes.toBytes("columnFamily"))
scan.addColumn(Bytes.toBytes("columnFamily"), Bytes.toBytes("columnQualifier"))
scan.addColumn(Bytes.toBytes("columnFamily2"), Bytes.toBytes("columnQualifier"))
val proto = ProtobufUtil.toScan(scan)

val scanStr = Base64.encodeBytes(proto.toByteArray())

conf.set(TableInputFormat.INPUT_TABLE, "TableName")
conf.set(TableInputFormat.SCAN, scanStr)

val rdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).asInstanceOf[NewHadoopRDD[ImmutableBytesWritable, Result]]

val rdd3 = rdd.map(t => t._2).map(result => (Bytes.toString(result.getValue("columnFamily2".getBytes,"columnQualifier".getBytes()))))

val rdd4 = rdd3.filter(line => line.contains("StringWeAreLookingFor"))

val rdd5 = rdd4.map(line =>  {
  val splits = line.split("\\|")
  (splits(6), line)
})

val rdd6 = rdd5.flatMap(t =>
  t._2.split("\r").toList.filter(line => line.startsWith("LinePrefix")).map(line=>(t._1,line))
).map(t2 => {
  val splits = t2._2.split("\\|")
  (t2._1,splits(3), splits(5), splits(6))
})

import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

val rdd7 = rdd6.map( k => {
  val v1 = if (k._2.length > 1) k._2.split("\\^")(1) else ""
  val v2 = if (k._4.length > 1) k._4.split("\\^")(1) else ""
  val json: JObject = ("PayLoadName" ->
    ("timestamp" -> k._1) ~
      ("type" -> v1) ~
      ("value" ->
            ("v" -> k._3) ~
            ("unit" -> v2)
      )
    )
  compact(render(json))
})

rdd7.saveAsTextFile("mytextFile")