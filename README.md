# Notes

This project is built with SBT (verified working with version 1.1.2).
It requires:

 - Java 8
 - Scala 2.12.6
 
## Design

The solution uses Akka streams to process the incoming messages. The example application reads its input from a file but any Akka streams input would work as well.

 - Messages are deserialized from Json using Spray.
 - Corrupt messages are discarded.
 - Messages are windowed by their session ID and the latest update, within the expiry time of the session, is used.
 - Summaries are then windowed by hour and grouped by document to produce the output aggregations.
 
Currently the aggregates are written to the console but any Akka streams sink would work.

## Usage

The example application is:

```com.example.analytics.Main```

Using sbt it can be run with:

```sbt "run --input test-visit-messages.log"```

More options are available. Run without parameters for full usage.

## Assumptions

The software makes the following assumptions about the data.

 - Updates always occur after corresponding create events.
 - Records are almost always in order. This can be violated to a small extent for updates.
 - Updates do not arrive after the session expiry time.
 - The number of active sessions and documents viewed in an hour is bounded above. These bounds are configurable.
 - Sessions that receive no updates are still counted towards views and unique views.
 
## Logging

The software uses Logback. The following circumstances are logged:

 - Corrupt records are logged as errors.
 - Updates with no corresponding create events are logged as warnings.
 - Severely out of order records are logged as warnings.
 - No nominal data is logged due to the volume.
 
Logging is configured in:

```src/main/resources/logback.xml```

By default, it is set to log to STDERR at ERROR.

## Testing

Tests are written using ScalaTest. They can be executed by:

```sbt test```

