package com.permutive.analytics.exceptions

/**
  * Exception thrown when a corrupt Json record is encountered.
  * @param msg The error.
  * @param cause The cause of the error.
  */
case class CorruptRecordException(msg : String, cause : Throwable) extends RuntimeException(msg, cause) {

  def this(msg : String) = this(msg, null)

}

object CorruptRecordException {
  /**
    * Create an exception with just a message.
    * @param msg The message.
    * @return The exception.
    */
  def apply(msg : String) : CorruptRecordException = new CorruptRecordException(msg)
}
