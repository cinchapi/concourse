import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.core.FileAppender

import static ch.qos.logback.classic.Level.INFO

appender("FILE", FileAppender) {
	file = "concourse.log"
	append = true
	encoder(PatternLayoutEncoder) {
	  pattern = "%level %logger - %msg%n"
	}
  }

root(INFO, ["FILE"])