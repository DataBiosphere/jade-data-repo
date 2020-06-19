package bio.terra.integration;

import bio.terra.common.category.Integration;
import bio.terra.common.fixtures.KubeFixture;
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

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class KubeTests {
    private static final Logger logger = LoggerFactory.getLogger(KubeTests.class);
    @Autowired
    private KubeFixture kubeFixture;

    @Before
    public void setup() throws Exception {
    }

    @After
    public void teardown() throws Exception {
    }

    @Test
    public void testListAllPods() {
        List<String> podList = kubeFixture.listAllPods();
        if (podList == null) {
            logger.info("TestListAllPods: no pods.");
        } else {
            for (String pod : podList) {
                logger.info("TestListAllPods: {}", pod);
            }
        }
    }
}
