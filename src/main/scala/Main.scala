import java.time.Duration
import java.util.Properties

import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import play.api.libs.json._
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroSerializer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


object Main extends App {

  import org.apache.kafka.streams.scala.serialization.Serdes._
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.kstream.{TimeWindows, Windowed, SlidingWindows}
  var motion = 0;
  println("Preparing")
    val config: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "cat-scala-application")
    val bootstrapServers = if (args.length > 0) args(0) else "iot-kafka-0.iot-kafka-headless.iot:9092,iot-kafka-1.iot-kafka-headless.iot:9092,iot-kafka-2.iot-kafka-headless.iot:9092"
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    p
  }
  val builder = new StreamsBuilder()
  val cat_values_matth: KStream[String, String] = builder.stream[String, String]("cat-values-all")

  var avg = 84.5
  def dumpKV[K,V](k:K,v:V):(K,V) = {
    println(s"key=$k, value=$v")
    (k,v)
  }
  

  case class SensorAll(NO2: Int, C2H5OH: Int,VOC: Int,CO: Int,temp: Double,humidity: Double, airQuality_in_ppm: Int ,motion: Int, warninglevel_styria: Int, windowOpen:Int = 0){
    override def toString: String =
    s"""{"NO2":$NO2,"C2H5OH":$C2H5OH,"VOC":$VOC,"CO":$CO,"humidity":$humidity,"temp":$temp,"warninglevel_styria":$warninglevel_styria,"airQuality_in_ppm":$airQuality_in_ppm,"motion":$motion, "windowOpen":$windowOpen}"""

  }

  def cleanString(s:String):String = {
    s.replace("\u0000","").replace("\u0002","")
  }

  def stringToSensorAll(s:String):SensorAll = {
    val json:JsValue = Json.parse(cleanString(s))
    var windowOpen = 0
    
    if (cleanString(s) contains "windowOpen"){
      windowOpen = (json \ "windowOpen").as[Int]
    }
    

    SensorAll( 
      (json \ "NO2").as[Int],
      (json \ "C2H5OH").as[Int],
      (json \ "VOC").as[Int], 
      (json \ "CO").as[Int],
      (json \ "temp").as[Double], 
      (json \ "humidity").as[Double],
      (json \ "airQuality_in_ppm").as[Int],
      (json \ "motion").as[Int], 
      (json \ "warninglevel_styria").as[Int],
      windowOpen
    )
  }

  def calcAvgAirQuality(s: String):Double = {
    val v = stringToSensorAll(s)
    (v.NO2 + v.C2H5OH + v.VOC + (3 * v.CO) + v.airQuality_in_ppm) / 7
  }

  def calcOutput(stream: String):String = {
    val streamVal = stringToSensorAll(stream)
    val motion = {if(streamVal.motion > 0){1} else {0}}
    val windowOpen = {if(streamVal.windowOpen > 0){1} else {0}}
    val covidFactor = ((streamVal.warninglevel_styria - 1) / 10) + 1
    val airQualityCalc = BigDecimal(((calcAvgAirQuality(stream) / (avg * 1.5)) * covidFactor)).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble

    val out = calcAvgAirQuality(stream)
    println(s"avg=$avg, value=$out ########################### $stream")


    val openWindow = {if(airQualityCalc > 1 && motion == 1 && windowOpen == 0){1} else {0}}

    s"""{"AirQualityCalc":$airQualityCalc,"OpenWindows": $openWindow,"motion":$motion, "windowIsOpen":$windowOpen}"""
  }

  def aggregateFunction(calc:String, datapoint:String): String = {
    if (calc == ""){return cleanString(datapoint)}
    val calcSensor = stringToSensorAll(calc)
    val datapointSensor = stringToSensorAll(datapoint)

    val windowOpenFactor = calcAvgAirQuality(datapoint) / calcAvgAirQuality(calc)
    val windowOpen = {if (windowOpenFactor < 0.6){10}else if(calcSensor.windowOpen > 0){calcSensor.windowOpen-1}else{0}}    // detects a 40% drop in ppm over the last 10 min


    val result = SensorAll( 
      ((calcSensor.NO2*9 + datapointSensor.NO2)/10).toInt,
      ((calcSensor.C2H5OH*9 + datapointSensor.C2H5OH)/10).toInt,
      ((calcSensor.VOC*9 + datapointSensor.VOC)/10).toInt,
      ((calcSensor.CO*9 + datapointSensor.CO)/10).toInt,
      (calcSensor.temp*9 + datapointSensor.temp)/10, 
      (calcSensor.humidity*9 + datapointSensor.humidity)/10,
      ((calcSensor.airQuality_in_ppm*9 + datapointSensor.airQuality_in_ppm)/10).toInt,
      {if(datapointSensor.motion == 1){10}else if(calcSensor.motion > 0){calcSensor.motion -1}else{0}}, 
      datapointSensor.warninglevel_styria,
      windowOpen
    )

    println(result.toString)
    result.toString
  }

  def stringToJson(s:String):JsValue = {
    Json.parse(s)
  }

  def calcLongtermAvg(a:Double,d:Double):Double = {
    avg = a
    (a * 1439 + d) / 1440
  }
  

  println("Starting")
  
  //val AirQualityCalculated: KStream[String, String] = cat_values_matth.map((k,v)=> dumpKV(k,v)).map((a,b)=>masterOfDesaster(a,b))

  val groupedStream: KGroupedStream[String, String] = cat_values_matth.groupByKey
  val longTermGroupedStream: KGroupedStream[String, Double] = cat_values_matth.map((k,v) => (k,calcAvgAirQuality(v))).groupByKey

  val windowedStream = groupedStream
  .windowedBy(TimeWindows.of(Duration.ofMinutes(10)))
  val longtermWindowedStream = longTermGroupedStream
  .windowedBy(TimeWindows.of(Duration.ofDays(1)))

  longtermWindowedStream.aggregate(84.5)((_, datapoint, aggregate) => {
      calcLongtermAvg(aggregate, datapoint)
    })

  val aggregatedStream: KTable[Windowed[String], String] = windowedStream
    .aggregate("")((_, datapoint, aggregate) => {
      aggregateFunction(aggregate, datapoint)
    })
  aggregatedStream.toStream.map((key, value) => (key.key(), calcOutput(value))).to("cat-values-04-output")


  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)

  streams.cleanUp()

  streams.start()
  
  
  // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }
}