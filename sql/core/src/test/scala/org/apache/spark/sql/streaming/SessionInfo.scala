package org.apache.spark.sql.streaming


  case class SessionInfo(
                          numEvents: Int,
                          startTimestampMs: Long,
                          endTimestampMs: Long) {

    /** Duration of the session, between the first and last events */
    def durationMs: Long = endTimestampMs - startTimestampMs
  }

  case class SessionUpdate(
                            id: String,
                            startTimestampMs: Long,
                            endTimestampMs: Long,
                            durationMs: Long,
                            numEvents: Int,
                            expired: Boolean)
