
import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.lang.Long._
import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.NewHadoopRDD

object App {


  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Spark Jar")
    val sc = new SparkContext(sparkConf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    
    println("file:" + args(3))

    class JobWrapper(sc:SparkContext) extends java.io.Serializable {  val job = Job.getInstance(sc.hadoopConfiguration) }
    @transient val job = new JobWrapper(sc)
    @transient val conf = job.job.getConfiguration
    HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf))

    HBaseAdmin.checkHBaseAvailable(conf)

    val scan = new Scan()
    val proto = ProtobufUtil.toScan(scan)
    val scanStr = Base64.encodeBytes(proto.toByteArray())

    conf.set(TableInputFormat.INPUT_TABLE, "TableName")
    conf.set(TableInputFormat.SCAN, scanStr)

    val rdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      .asInstanceOf[NewHadoopRDD[ImmutableBytesWritable, Result]]

    val rdd2 = rdd.map(tuple => tuple._2)
      .map(result => (Bytes.toString(result.getValue("cf".getBytes(), "qual".getBytes()))))

   
  }

  //add functions here
}