package actors

import akka.actor.{ActorSystem, Cancellable}
import play.api.db.Database
import play.api.i18n.Lang.logger
import service.DeliveryEventConsumer

import javax.inject.{Inject, Singleton}
import scala.concurrent.duration.{FiniteDuration, SECONDS}
import scala.util.Success

@Singleton
class DeliveryJournalActor @Inject()(system: ActorSystem,
                                     deliveryEventConsumer: DeliveryEventConsumer,
                                     override val db: Database)
  extends DBPollActor(table = "deliveries") {

  println("actor initialized")

  override def preStart(): Unit = {
    logger.info("[DeliveryJournalActor] Inside preStart")
    startPolling()
    deliveryEventConsumer.initialize()
  }

  def schedule(): Cancellable = {
    system.scheduler.scheduleWithFixedDelay(FiniteDuration(5, SECONDS), delay, self, "INSERT")(system.dispatcher)
  }

  override def process(record: ProcessQueueDelivery): Unit = {
    record.operation match {
      case "INSERT" | "UPDATE" =>
        //throw new ArithmeticException("Error occur")
      case "DELETE" => Success(())
    }
  }
}