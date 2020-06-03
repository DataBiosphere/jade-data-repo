package bio.terra.integration;

import bio.terra.common.category.Integration;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.kubernetes.KubeService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class KubeServiceTest extends UsersBase {
    private static final Logger logger = LoggerFactory.getLogger(KubeServiceTest.class);
    private static final int POD_LISTENER_SHUTDOWN_TIMEOUT = 2;

    @Autowired
    private KubeService kubeService;
    @Autowired
    private ConfigurationService configurationService;

    @Before
    public void setup() throws Exception {
        super.setup();
    }

    @After
    public void teardown() throws Exception {
    }

    @Test
    public void testPodCount() throws Exception {
        try {
            kubeService.stopPodListener(TimeUnit.SECONDS, POD_LISTENER_SHUTDOWN_TIMEOUT);
            kubeService.startPodListener();
            int podCount = kubeService.getActivePodCount();
            logger.info("testPodCount: podCount: {};", podCount);
            assertThat("pod count should be 1", podCount, equalTo(1));
            int concurrentFiles = configurationService.getParameterValue(ConfigEnum.LOAD_CONCURRENT_FILES);
            logger.info("testPodCount: concurrentFiles: {};", concurrentFiles);
            int scaledConcurrentFiles = podCount * concurrentFiles;
            logger.info("testPodCount: scaledConcurrentFiles: {}", scaledConcurrentFiles);
        } catch (Exception ex) {
            logger.info("testPodCount Error: {}", ex);
        }
    }
}
