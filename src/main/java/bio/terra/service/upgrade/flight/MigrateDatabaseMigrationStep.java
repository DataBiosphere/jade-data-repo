package bio.terra.service.upgrade.flight;

import bio.terra.model.UpgradeModel;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.job.JobService;
import bio.terra.service.upgrade.MigrateConfiguration;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import static java.lang.Boolean.parseBoolean;

public class MigrateDatabaseMigrationStep implements Step {
    private final Logger logger = LoggerFactory.getLogger(MigrateDatabaseMigrationStep.class);
    private final UpgradeModel request;
    private final AuthenticatedUserRequest user;
    private final JobService jobService;
    private final MigrateConfiguration migrateConfiguration;

    public MigrateDatabaseMigrationStep(JobService jobService,
                                        MigrateConfiguration migrateConfiguration,
                                        UpgradeModel request,
                                        AuthenticatedUserRequest user) {
        this.migrateConfiguration = migrateConfiguration;
        this.jobService = jobService;
        this.request = request;
        this.user = user;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        List<String> customArgs = request.getCustomArgs();
        boolean dropAllOnStart = false;
        if (customArgs != null && customArgs.size() > 0) {
            dropAllOnStart = parseBoolean(customArgs.get(0));
        }
        logger.info("MigrateDatabaseMigrationStep - custom argument for dropAllOnStart: {}", dropAllOnStart);
        migrateConfiguration.setDropAllOnStart(dropAllOnStart);
        jobService.initialize();
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        return StepResult.getStepResultSuccess();
    }
}
