package com.permutive.analytics.windows

import java.time.Instant

import com.permutive.analytics.model.Timestamped

case class TimeRecord(timestamp : Instant, n : Int = 0) extends Timestamped
