package bio.terra.app.logging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Primary
@Profile({"perftest"})
@Component
public class PerformanceLoggerON implements PerformanceLogger {
    private ObjectMapper objectMapper;

    private static Logger logger = LoggerFactory.getLogger(PerformanceLoggerON.class);
    private static final String PerformanceLogFormat =
        "TimestampUTC: {}, JobId: {}, Class: {}, Operation: {}, " +
        "ElapsedTime: {}, IntegerCount: {}, AdditionalInfo: {}";

    private static final ThreadLocal<Map<String, Instant>> startTimeMap =
        ThreadLocal.withInitial(() -> new HashMap<>());

    @Autowired
    public PerformanceLoggerON(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        logger.info("Performance logging ON");
    }

    public void log(String jobId, String className, String operationName,
                    long elapsedTime, long integerCount, Serializable additionalInfo) {
        // serialize the additional info object to JSON
        String additionalInfoStr = "";
        if (additionalInfo != null) {
            try {
                additionalInfoStr = objectMapper.writeValueAsString(additionalInfo);
            } catch (JsonProcessingException jsonEx) {
                additionalInfoStr = "Failure serializing additionalInfo to JSON.";
            }
        }

        logger.info(PerformanceLogFormat, currentTimestamp(), jobId, className, operationName,
            elapsedTime, integerCount, additionalInfoStr);
    }

    private String currentTimestamp() {
        return Instant.now().atZone(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
    }

    public String timerStart() {
        Instant startTime = Instant.now();
        String timerId = UUID.randomUUID().toString();
        startTimeMap.get().put(timerId, startTime);
        return timerId;
    }

    public void timerStart(String timerId) {
        Instant startTime = Instant.now();
        startTimeMap.get().put(timerId, startTime);
    }

    public void timerEndAndLog(String timerId, String jobId, String className, String operationName,
                               long integerCount, Serializable additionalInfo) {
        Instant endTime = Instant.now();
        Instant startTime = startTimeMap.get().remove(timerId);
        long elapsedTimeNS = Duration.between(startTime, endTime).getNano();

        log(jobId, className, operationName, elapsedTimeNS, integerCount, additionalInfo);
    }
}
