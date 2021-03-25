package bio.terra.service.upgrade.flight;

import bio.terra.model.UpgradeModel;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.upgrade.MigrateConfiguration;
import bio.terra.stairway.FlightContext;
import bio.terra.service.upgrade.Migrate;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.List;

import static java.lang.Boolean.getBoolean;
import static java.lang.Boolean.parseBoolean;

public class MigrateDataRepoDatabaseStep implements Step {
    private final UpgradeModel request;
    private final AuthenticatedUserRequest user;
    private final Migrate migrate;
    private final MigrateConfiguration migrateConfiguration;

    public MigrateDataRepoDatabaseStep(Migrate migrate,
                                       MigrateConfiguration migrateConfiguration,
                                       UpgradeModel request,
                                       AuthenticatedUserRequest user) {
        this.migrateConfiguration = migrateConfiguration;
        this.migrate = migrate;
        this.request = request;
        this.user = user;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        List<String> customArgs = request.getCustomArgs();
        boolean dropAllOnStart = false;
        if (customArgs != null && customArgs.size() > 0 && parseBoolean(customArgs.get(0))) {
            dropAllOnStart = getBoolean(customArgs.get(0));
        }
        migrateConfiguration.setDropAllOnStart(dropAllOnStart);
        migrate.migrateDatabase();
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        return StepResult.getStepResultSuccess();
    }
}
