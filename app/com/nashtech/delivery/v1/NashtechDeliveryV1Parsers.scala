/**
 * Generated by API Builder - https://www.apibuilder.io
 * Service version: 1.0.1
 * apibuilder 0.15.33 app.apibuilder.io/nashtech/delivery/latest/anorm_2_x_parsers
 */
import anorm._

package com.nashtech.delivery.v1.anorm.parsers {

  import com.nashtech.delivery.v1.anorm.conversions.Standard._

  import com.nashtech.delivery.v1.anorm.conversions.Types._

  object Address {

    def parserWithPrefix(prefix: String, sep: String = "_"): RowParser[com.nashtech.delivery.v1.models.Address] = parser(prefixOpt = Some(s"$prefix$sep"))

    def parser(
      text: String = "text",
      streets: String = "streets",
      streetNumber: String = "street_number",
      city: String = "city",
      province: String = "province",
      postal: String = "postal",
      country: String = "country",
      latitude: String = "latitude",
      longitude: String = "longitude",
      prefixOpt: Option[String] = None
    ): RowParser[com.nashtech.delivery.v1.models.Address] = {
      SqlParser.str(prefixOpt.getOrElse("") + text).? ~
      SqlParser.get[Seq[String]](prefixOpt.getOrElse("") + streets).? ~
      SqlParser.str(prefixOpt.getOrElse("") + streetNumber).? ~
      SqlParser.str(prefixOpt.getOrElse("") + city).? ~
      SqlParser.str(prefixOpt.getOrElse("") + province).? ~
      SqlParser.str(prefixOpt.getOrElse("") + postal).? ~
      SqlParser.str(prefixOpt.getOrElse("") + country).? ~
      SqlParser.str(prefixOpt.getOrElse("") + latitude).? ~
      SqlParser.str(prefixOpt.getOrElse("") + longitude).? map {
        case text ~ streets ~ streetNumber ~ city ~ province ~ postal ~ country ~ latitude ~ longitude => {
          com.nashtech.delivery.v1.models.Address(
            text = text,
            streets = streets,
            streetNumber = streetNumber,
            city = city,
            province = province,
            postal = postal,
            country = country,
            latitude = latitude,
            longitude = longitude
          )
        }
      }
    }

  }

  object Contact {

    def parserWithPrefix(prefix: String, sep: String = "_"): RowParser[com.nashtech.delivery.v1.models.Contact] = parser(prefixOpt = Some(s"$prefix$sep"))

    def parser(
      firstName: String = "first_name",
      lastName: String = "last_name",
      mobileNumber: String = "mobile_number",
      emailId: String = "email_id",
      prefixOpt: Option[String] = None
    ): RowParser[com.nashtech.delivery.v1.models.Contact] = {
      SqlParser.str(prefixOpt.getOrElse("") + firstName).? ~
      SqlParser.str(prefixOpt.getOrElse("") + lastName).? ~
      SqlParser.str(prefixOpt.getOrElse("") + mobileNumber).? ~
      SqlParser.str(prefixOpt.getOrElse("") + emailId).? map {
        case firstName ~ lastName ~ mobileNumber ~ emailId => {
          com.nashtech.delivery.v1.models.Contact(
            firstName = firstName,
            lastName = lastName,
            mobileNumber = mobileNumber,
            emailId = emailId
          )
        }
      }
    }

  }

  object Delivery {

    def parserWithPrefix(prefix: String, sep: String = "_"): RowParser[com.nashtech.delivery.v1.models.Delivery] = parser(prefixOpt = Some(s"$prefix$sep"))

    def parser(
      id: String = "id",
      orderNumber: String = "order_number",
      merchantId: String = "merchant_id",
      estimatedDeliveryDate: String = "estimated_delivery_date",
      originPrefix: String = "origin",
      destinationPrefix: String = "destination",
      contactInfoPrefix: String = "contact_info",
      prefixOpt: Option[String] = None
    ): RowParser[com.nashtech.delivery.v1.models.Delivery] = {
      SqlParser.str(prefixOpt.getOrElse("") + id) ~
      SqlParser.str(prefixOpt.getOrElse("") + orderNumber) ~
      SqlParser.str(prefixOpt.getOrElse("") + merchantId) ~
      SqlParser.get[_root_.org.joda.time.DateTime](prefixOpt.getOrElse("") + estimatedDeliveryDate) ~
      com.nashtech.delivery.v1.anorm.parsers.Address.parserWithPrefix(prefixOpt.getOrElse("") + originPrefix) ~
      com.nashtech.delivery.v1.anorm.parsers.Address.parserWithPrefix(prefixOpt.getOrElse("") + destinationPrefix) ~
      com.nashtech.delivery.v1.anorm.parsers.Contact.parserWithPrefix(prefixOpt.getOrElse("") + contactInfoPrefix) map {
        case id ~ orderNumber ~ merchantId ~ estimatedDeliveryDate ~ origin ~ destination ~ contactInfo => {
          com.nashtech.delivery.v1.models.Delivery(
            id = id,
            orderNumber = orderNumber,
            merchantId = merchantId,
            estimatedDeliveryDate = estimatedDeliveryDate,
            origin = origin,
            destination = destination,
            contactInfo = contactInfo
          )
        }
      }
    }

  }

  object DeliveryForm {

    def parserWithPrefix(prefix: String, sep: String = "_"): RowParser[com.nashtech.delivery.v1.models.DeliveryForm] = parser(prefixOpt = Some(s"$prefix$sep"))

    def parser(
      orderNumber: String = "order_number",
      merchantId: String = "merchant_id",
      originPrefix: String = "origin",
      destinationPrefix: String = "destination",
      contactInfoPrefix: String = "contact_info",
      prefixOpt: Option[String] = None
    ): RowParser[com.nashtech.delivery.v1.models.DeliveryForm] = {
      SqlParser.str(prefixOpt.getOrElse("") + orderNumber) ~
      SqlParser.str(prefixOpt.getOrElse("") + merchantId) ~
      com.nashtech.delivery.v1.anorm.parsers.Address.parserWithPrefix(prefixOpt.getOrElse("") + originPrefix) ~
      com.nashtech.delivery.v1.anorm.parsers.Address.parserWithPrefix(prefixOpt.getOrElse("") + destinationPrefix) ~
      com.nashtech.delivery.v1.anorm.parsers.Contact.parserWithPrefix(prefixOpt.getOrElse("") + contactInfoPrefix) map {
        case orderNumber ~ merchantId ~ origin ~ destination ~ contactInfo => {
          com.nashtech.delivery.v1.models.DeliveryForm(
            orderNumber = orderNumber,
            merchantId = merchantId,
            origin = origin,
            destination = destination,
            contactInfo = contactInfo
          )
        }
      }
    }

  }

  object Error {

    def parserWithPrefix(prefix: String, sep: String = "_"): RowParser[com.nashtech.delivery.v1.models.Error] = parser(prefixOpt = Some(s"$prefix$sep"))

    def parser(
      code: String = "code",
      message: String = "message",
      prefixOpt: Option[String] = None
    ): RowParser[com.nashtech.delivery.v1.models.Error] = {
      SqlParser.str(prefixOpt.getOrElse("") + code) ~
      SqlParser.str(prefixOpt.getOrElse("") + message) map {
        case code ~ message => {
          com.nashtech.delivery.v1.models.Error(
            code = code,
            message = message
          )
        }
      }
    }

  }

}