package bio.terra.integration;

import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public final class TestUtils {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(TestUtils.class);

    private TestUtils() {}

    public static <T> boolean flappyExpect(
        int secInterval, int secTimeout, T expected, Callable<T> callable) throws Exception {
        LocalDateTime end = LocalDateTime.now().plus(Duration.ofSeconds(secTimeout));
        int tries = 0;
        while (LocalDateTime.now().isBefore(end)) {
            String logging = String.format("Time elapsed: %03d seconds, Tried: %03d times", secInterval * tries, tries);
            logger.info(logging);
            if (callable.call().equals(expected)) {
                return true;
            }
            TimeUnit.SECONDS.sleep(secInterval);
            tries++;
        }
        return false;
    }
}
