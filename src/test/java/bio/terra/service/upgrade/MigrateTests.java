package bio.terra.service.upgrade;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.configuration.StairwayJdbcConfiguration;
import bio.terra.app.logging.PerformanceLogger;
import bio.terra.common.category.Unit;
import bio.terra.service.iam.IamService;
import bio.terra.service.job.JobService;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(SpringRunner.class)
@Category(Unit.class)
@ActiveProfiles({"google", "test"})
public class MigrateTests {

    private IamService samService;
    private ApplicationConfiguration appConfig;
    private StairwayJdbcConfiguration stairwayJdbcConfiguration;
    private ApplicationContext applicationContext;
    private ObjectMapper objectMapper;
    private PerformanceLogger performanceLogger;
    private Migrate migrate;
    private JobService jobService;
    private MigrateConfiguration migrateConfiguration;
    private StandardEnvironment environment;
    private GoogleResourceConfiguration resourceConfiguration;



    @Before
    public void setup() throws Exception {
        environment = new org.springframework.core.env.StandardEnvironment();

        migrateConfiguration = new MigrateConfiguration();

        resourceConfiguration = new GoogleResourceConfiguration();

        samService = mock(IamService.class);
        appConfig = mock(ApplicationConfiguration.class);
        stairwayJdbcConfiguration = mock(StairwayJdbcConfiguration.class);
        applicationContext = mock(ApplicationContext.class);
        migrate = mock(Migrate.class);
        objectMapper = mock(ObjectMapper.class);
        performanceLogger = mock(PerformanceLogger.class);

        jobService = new JobService(samService,
            appConfig,
            stairwayJdbcConfiguration,
            migrateConfiguration,
            resourceConfiguration,
            applicationContext,
            migrate,
            objectMapper,
            performanceLogger,
            environment,
            resourceConfiguration);
    }



    @Test
    public void dropAllOnStartTrue() {
        environment.setActiveProfiles("test");
        migrateConfiguration.setDropAllOnStart(true);

        boolean allowDropAllOnStart = jobService.allowDropAllOnStart();
        assertTrue("allowDropAllOnStart should be true for test env when drop all set to true",
            allowDropAllOnStart);

        boolean dropAllOnStart = jobService.getDropAllOnStart();
        assertTrue("dropAllOnStart should be true for test env when drop all set to true",
            dropAllOnStart);
    }

    @Test
    public void dropAllOnStartFalse() {
        environment.setActiveProfiles("test");
        migrateConfiguration.setDropAllOnStart(false);

        boolean allowDropAllOnStart = jobService.allowDropAllOnStart();
        assertTrue("allowDropAllOnStart is true since on test environment",
            allowDropAllOnStart);

        boolean dropAllOnStart = jobService.getDropAllOnStart();
        assertFalse("Even though allowed, drop all on start is false when dropAllOnStart=false",
            dropAllOnStart);
    }

    @Test
    public void nonTestEnvironment() {
        environment.setActiveProfiles("Staging");
        migrateConfiguration.setDropAllOnStart(true);

        boolean allowDropAllOnStart = jobService.allowDropAllOnStart();
        assertFalse("allowDropAllOnStart should be false for non-test env even when drop all set to true",
            allowDropAllOnStart);

        boolean dropAllOnStart = jobService.getDropAllOnStart();
        assertFalse("dropAllOnStart should be false when AllowDropAllOnStart=false",
            dropAllOnStart);
    }

    @Test
    public void dataProjectNoDrop() {
        environment.setActiveProfiles("test");
        migrateConfiguration.setDropAllOnStart(true);
        String testNoDropDataProjectName =  "testNoDropDataProject";
        migrateConfiguration.setDataProjectNoDropAll(Collections.singletonList(testNoDropDataProjectName));

        //Test case 1 - setting the data project to match the project set as "no drop"
        // allow drop on start ==> false
        resourceConfiguration.setSingleDataProjectId(testNoDropDataProjectName);

        boolean allowDropAllOnStart = jobService.allowDropAllOnStart();
        assertFalse("allowDropAllOnStart should be false for " + testNoDropDataProjectName,
            allowDropAllOnStart);

        boolean dropAllOnStart = jobService.getDropAllOnStart();
        assertFalse("dropAllOnStart should be false when AllowDropAllOnStart=false",
            dropAllOnStart);


        //Test case 2 - setting the data project to something else that does NOT match the "no drop" project
        // allow drop on start ==> true
        String testDataProjectName =  "testDataProject";
        resourceConfiguration.setSingleDataProjectId(testDataProjectName);

        allowDropAllOnStart = jobService.allowDropAllOnStart();
        assertTrue("allowDropAllOnStart should be true for test data project " + testDataProjectName,
            allowDropAllOnStart);

        dropAllOnStart = jobService.getDropAllOnStart();
        assertTrue("dropAllOnStart should be true when AllowDropAllOnStart=true and dropAllOnStart=true",
            dropAllOnStart);
    }
}
