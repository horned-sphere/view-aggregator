package com.example.analytics.windows

import java.time.Instant

import com.example.analytics.model.Timestamped

case class TimeRecord(timestamp : Instant, n : Int = 0) extends Timestamped
