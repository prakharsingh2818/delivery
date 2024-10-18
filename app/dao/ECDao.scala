package dao

import anorm.{RowParser, SQL, SqlParser, ~}
import com.nashtech.order.v1.models.Order
import org.joda.time.DateTime
import play.api.db.Database

import javax.inject.Inject

class ECDao @Inject()(db: Database) {

  def createEcOrder(order: Order): Order = {
    db.withConnection {  implicit connection =>
      SQL(BaseQuery.insertQuery(order, order.merchantId)).as(OrderParser().single)
    }
  }

  private object BaseQuery {

    def insertQuery(order: Order, merchantId: String): String = {

      val query =
        s"""
           |INSERT INTO ec_orders
           |(
           |id,
           |number,
           |merchant_id,
           |total,
           |submitted_at
           |)
           |VALUES
           |(
           |'${order.id}',
           |'${order.number}',
           |'${merchantId}',
           |${order.total},
           |'${order.submittedAt}'
           |) ON CONFLICT (id)
           |DO UPDATE
           |set total = '${order.total}',
           |submitted_at = '${order.submittedAt}'::timestamptz
           |returning *
           |""".stripMargin
      query
    }
  }

  private def OrderParser(): RowParser[Order] = {
    SqlParser.str("id") ~
      SqlParser.str("number") ~
      SqlParser.str("merchant_id") ~
      SqlParser.get[DateTime]("submitted_at") ~
      SqlParser.double("total") map {

      case id ~ number ~ merchantId ~ submittedAt ~ total =>
        Order(
          id, number, merchantId, submittedAt, total
        )
    }
  }

}
