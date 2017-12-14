import io.circe.Json
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.http.client.methods._
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import io.circe.parser.parse

import scala.io.Source

object ApplicationMain {

  def main(args: Array[String]): Unit = {

    // Setup Spark Context
    val conf = new SparkConf().setAppName("KF-ETL").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val query: String =
      """
        |{"query": "{case(first:10000) {id}}"}
      """.stripMargin

    val gen3URL = "https://gen3.kids-first.io/api/v0/submission/graphql/"
    val request = new HttpPost(gen3URL)
    request.setEntity(new StringEntity(query))
    request.setHeader("Cookie", "csrftoken=fI24XxWnQqfAYkVu78S7SrN5PHx0rHzVgJJDkX1u; userapi_session=266cdac0-e400-469d-93d8-dabed6704459; session=eyJhY2Nlc3NfdG9rZW4iOiJ1a09xd21pSlp3N1FzSzM0WHZBczlkOWZtNzhxYUsifQ.DRMIPg.Ei0uAgRKFVwTefwT_rJmRq8Yeug")

    val response = HttpClients.createDefault.execute(request)
    val content = Source.fromInputStream(response.getEntity().getContent()).getLines.mkString

    parse(content) match {
      case Right(json) => {
        val idCollection =
          json.hcursor
            .downField("data")
            .downField("case")
            .downArray.rights.get.map(j => j.hcursor.downField("id").as[String].right.get)

        val c = sc.makeRDD(idCollection)
          .map( id => HttpClients.createDefault.execute((new HttpGet("https://gen3.kids-first.io/api/v0/submission/GMKF/PCGC2016/export?format=tsv&ids=" + id))).getStatusLine().getStatusCode()
          )
          .count()
        println(c.toString())
      }
      case Left(parseFailure) => throw parseFailure
    }


    println("end")
  }

}
