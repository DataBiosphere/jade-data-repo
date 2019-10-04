package bio.terra.integration;

import bio.terra.model.DRSAccessMethod;
import org.apache.commons.lang3.StringUtils;
import bio.terra.service.iam.SamClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public final class TestUtils {
    private static Logger logger = LoggerFactory.getLogger(TestUtils.class);

    private TestUtils() {}

    public static <T> boolean eventualExpect(
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

    public static String validateDrsAccessMethods(List<DRSAccessMethod> accessMethods) {
        assertThat("Two access methods", accessMethods.size(), equalTo(2));

        String gsuri = StringUtils.EMPTY;
        boolean gotGs = false;
        boolean gotHttps = false;
        for (DRSAccessMethod accessMethod : accessMethods) {
            if (accessMethod.getType() == DRSAccessMethod.TypeEnum.GS) {
                assertFalse("have not seen GS yet", gotGs);
                gotGs = true;
                gsuri = accessMethod.getAccessUrl().getUrl();
            } else if (accessMethod.getType() == DRSAccessMethod.TypeEnum.HTTPS) {
                assertFalse("have not seen HTTPS yet", gotHttps);
                gotHttps = true;
            } else {
                fail("Invalid access method");
            }
        }
        assertTrue("got both access methods", gotGs && gotHttps);
        return gsuri;
    }

    public static String getHttpPathString(SamClientService.ResourceType resourceType) {
        String httpPathString = null;
        switch (resourceType) {
            case DATASET:
                httpPathString = "datasets";
                break;
            case DATASNAPSHOT:
                httpPathString = "snapshots";
                break;
            default:
                httpPathString = null;
        }

        return httpPathString;
    }
}

