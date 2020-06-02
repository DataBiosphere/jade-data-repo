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

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class KubeServiceTest extends UsersBase {
    private static final Logger logger = LoggerFactory.getLogger(KubeServiceTest.class);
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
        int podCount = kubeService.getActivePodCount();
        int concurrentFiles = configurationService.getScaledValue(ConfigEnum.LOAD_CONCURRENT_FILES);
        int scaledConcurrentFiles = podCount * concurrentFiles;
        logger.info("podCount: {}; concurrentFiles: {}; scaledConcurrentFiles: {}",
            podCount, concurrentFiles, scaledConcurrentFiles);
    }
}
