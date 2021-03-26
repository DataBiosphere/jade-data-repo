package bio.terra.service.upgrade;

import bio.terra.common.category.Unit;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@Category(Unit.class)
@SpringBootTest
@ActiveProfiles({"google", "test"})
@AutoConfigureMockMvc
public class MigrateTests {

    @Autowired
    Migrate migrate;

    @Autowired
    private GoogleResourceConfiguration googleResourceConfiguration;

    private String singleDataProject;
    @Before
    public void setup() throws Exception {
        singleDataProject = googleResourceConfiguration.getSingleDataProjectId();
    }

    @After
    public void teardown() {
        if (singleDataProject != null){
            googleResourceConfiguration.setSingleDataProjectId(singleDataProject);
        }
    }
    @Test
    public void testAllowDropAllOnStartTrue(){
        boolean allowDropAllOnStart = migrate.allowDropAllOnStart();
        assertTrue("allowDropAllOnStart should be true on test environments", allowDropAllOnStart);
    }

    @Ignore("If it doesn't properly clean up, then will have set the tests to use the dev-data project")
    @Test
    public void testAllowDropAllOnStartFalse(){
        googleResourceConfiguration.setSingleDataProjectId("broad-jade-dev-data");
        boolean allowDropAllOnStart = migrate.allowDropAllOnStart();
        assertFalse("allowDropAllOnStart should be false for broad-jade-dev-data", allowDropAllOnStart);
    }
}
