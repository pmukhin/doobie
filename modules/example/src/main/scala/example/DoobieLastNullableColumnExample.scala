// Copyright (c) 2013-2018 Rob Norris and Contributors
// This software is licensed under the MIT License (MIT).
// For more information see LICENSE or https://opensource.org/licenses/MIT

package example

import cats.effect._
import cats.implicits._
import doobie.implicits._
import doobie.{ConnectionIO, Transactor}

@SuppressWarnings(
  Array(
    "org.wartremover.warts.JavaConversions",
    "org.wartremover.warts.MutableDataStructures"
  )
)
object DoobieLastNullableColumnExample extends IOApp {

  final case class City(id: Int,
                        name: String,
                        countrycode: Option[String],
                        population: Option[Int])

  /** Table creation for our destination DB. We assume the source is populated. */
  val ddl: ConnectionIO[Unit] =
    sql"""
      CREATE TABLE IF NOT EXISTS City (
          id integer NOT NULL,
          name varchar NOT NULL,
          countrycode character(3),
          population integer
      )
    """.update.run.void

  val xa = Transactor.fromDriverManager[IO](
    "org.h2.Driver",
    "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1",
    "sa",
    ""
  )

  private val data =
    Seq(
      City(1, "Moscow", Some("RU"), Some(18000000)),
      City(2, "Tbilisi", Some("GE"), None),
      City(3, "Minsk", Some("BE"), None),
      City(4, "Singapore", None, None)
    )

  private def selectOneNullable =
    sql"select * from City where id = 4".query[City].option

  import io.getquill._

  val dc = new doobie.quill.DoobieContext.MySQL(Literal)
  import dc._

  private def insert =
    dc.run(quote(liftQuery(data).foreach(c => query[City].insert(c))))

  private def selectQuill =
    dc.run(quote(query[City]))

  override def run(args: List[String]): IO[ExitCode] =
    ddl.transact(xa) *>
      insert.transact(xa) *>
      selectQuill.transact(xa).map(println) *>
      selectOneNullable.transact(xa).map(println) *>
      IO.pure(ExitCode.Success)

}
