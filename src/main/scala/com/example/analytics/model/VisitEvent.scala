package com.example.analytics.model

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.UUID

import com.example.analytics.exceptions.CorruptRecordException
import spray.json.{DefaultJsonProtocol, JsObject, JsString, JsValue, JsonFormat, RootJsonFormat}

import scala.util.{Failure, Success, Try}

/**
  * Type for events representing the interaction of a user with a document.
  */
sealed trait VisitEvent extends Timestamped {

  /**
    * @return Unique ID for the users session.
    */
  def id : UUID

}

/**
  * Event indicating that a new session has started.
  * @param id The unique ID for the session.
  * @param userId The ID of the user.
  * @param documentId The ID of the document.
  * @param createdAt The time the session started.
  */
case class VisitCreate(id : UUID,
                       userId : UUID,
                       documentId : UUID,
                       createdAt : Instant) extends VisitEvent {
  override def timestamp: Instant = createdAt
}

/**
  * Updated information regarding a session.
  * @param id The unique ID of the session.
  * @param engagedTime The total engaged time in seconds.
  * @param completion The proportion of the document that the user has completed.
  * @param updatedAt The time that this updated was produced.
  */
case class VisitUpdate(id : UUID,
                       engagedTime : Int,
                       completion : Double,
                       updatedAt : Instant) extends VisitEvent {
  override def timestamp: Instant = updatedAt
}

/**
  * Dummy event to flush the stream at the end of finite inputs.
  */
case object Flush extends VisitEvent {
  override def id: UUID = UUID.randomUUID()

  override def timestamp: Instant = Instant.MAX
}

object VisitEvent {

  /**
    * Spray Json protocol to deserialize messages.
    */
  object VisitEventJsonProtocol extends DefaultJsonProtocol {

    implicit object UuidFormat extends JsonFormat[UUID] {

      def write(id: UUID): JsValue = JsString(id.toString)

      def read(json: JsValue): UUID = json match {
        case JsString(rep) => try{
          UUID.fromString(rep)
        } catch {
          case ex : Exception => throw CorruptRecordException(s"Invalid UUID: $rep.", ex)
        }
        case _ => throw CorruptRecordException(s"$json cannot be parsed as a UUID.")
      }
    }

    /**
      * Spray format for [[Instant]]s
      */
    implicit object InstantFormat extends JsonFormat[Instant] {

      private val Format = DateTimeFormatter.ISO_INSTANT

      override def write(obj: Instant): JsValue = JsString(Format.format(obj))

      override def read(json: JsValue): Instant = json match {
        case JsString(rep) => try {
          Instant.parse(rep)
        } catch {
          case ex : Exception => throw CorruptRecordException(s"Invalid ISO instant: $rep.", ex)
        }
        case _ => throw CorruptRecordException(s"$json cannot be parsed as an Instant.")
      }
    }

    implicit val createFormat : RootJsonFormat[VisitCreate] = jsonFormat4(VisitCreate.apply)
    implicit val updateFormat: RootJsonFormat[VisitUpdate] = jsonFormat4(VisitUpdate.apply)

    /**
      * Spray format to deserialize [[VisitEvent]]s.
     */
    implicit object VisitEventFormat extends RootJsonFormat[VisitEvent] {
      override def write(obj: VisitEvent): JsValue = obj match {
        case create : VisitCreate => JsObject(
          MessageTypeField -> JsString(VisitCreateName),
          MessageField -> createFormat.write(create)
        )
        case update : VisitUpdate => JsObject(
          MessageTypeField -> JsString(VisitUpdateName),
          MessageField -> updateFormat.write(update)
        )
        case _ => throw new IllegalArgumentException("Flush events cannot be serialized.")
      }

      override def read(json: JsValue): VisitEvent = json match {
        case JsObject(fields) =>
          (fields.get(MessageTypeField), fields.get(MessageField)) match {
            case (Some(JsString(VisitCreateName)), Some(body)) => createFormat.read(body)
            case (Some(JsString(VisitUpdateName)), Some(body)) => updateFormat.read(body)
            case _ => throw CorruptRecordException(s"Invalid message: $json")
          }
        case _ => throw CorruptRecordException("A message must consist of a Json object.")
      }
    }

  }

  /**
    * The name of the message type field in the Json documents.
    */
  final val MessageTypeField = "messageType"

  /**
    * The name of the field containing the message body in the Json documents.
    */
  final val MessageField = "visit"

  /**
    * Tag for create events.
    */
  final val VisitCreateName = "VisitCreate"

  /**
    * Tag for update events.
    */
  final val VisitUpdateName = "VisitUpdate"

  /**
    * Validate an event to ensure it makes sense.
    * @param event The event.
    * @return The event or a list of errors.
    */
  def validateEvent(event : VisitEvent) : Either[VisitEvent, List[String]] = {
    event match {
      case VisitUpdate(id, engagedTime, completion, ts) =>
        val engagedErr = if (engagedTime < 0)
          s"Engaged time for $id at $ts is negative: $engagedTime" :: Nil
        else Nil
        val compErr = if (completion < 0.0 || completion > 1.0)
          s"Completion fpr $id at $ts is not in the range [0.0, 1.0]." :: engagedErr
        else engagedErr

        if (compErr.nonEmpty) Right(compErr) else Left(event)
      case ev => Left(ev)
    }
  }

  /**
    * Parse an event from a Json document.
    * @param body The Json body.
    * @return The event or a list of errors.
    */
  def parse(body : String) : Either[VisitEvent, List[String]] = {
    import VisitEventJsonProtocol._
    import spray.json._
    Try(body.parseJson.convertTo[VisitEvent]) match {
      case Success(event) => validateEvent(event)
      case Failure(t) => Right(List(t.getMessage))
    }


  }

  /**
    * Get the created at time of an event, where possible.
    * @param event The event.
    * @return The time it was created.
    */
  def createdAt(event : VisitEvent) : Option[Instant] = event match {
    case VisitCreate(_, _, _, created) => Some(created)
    case _ => None
  }

}