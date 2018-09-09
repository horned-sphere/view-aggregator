package com.example.analytics.model

import java.time.Instant

/**
  * Trait for types that have an intrinsic timestamp.
  */
trait Timestamped {

  /**
    * @return The timestamp.
    */
  def timestamp : Instant


}
