package bio.terra.service.upgrade.flight;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.model.UpgradeModel;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.service.upgrade.Migrate;
import bio.terra.service.upgrade.MigrateConfiguration;
import org.springframework.context.ApplicationContext;

public class MigrateDatabaseFlight extends Flight {

    public MigrateDatabaseFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        ApplicationContext appContext = (ApplicationContext) applicationContext;
        Migrate migrate = (Migrate) appContext.getBean("migrate");
        MigrateConfiguration migrateConfiguration =
            (MigrateConfiguration) appContext.getBean("migrateConfiguration");
        IamService iamService =
            (IamService) appContext.getBean("iamService");
        ApplicationConfiguration applicationConfiguration =
            (ApplicationConfiguration) appContext.getBean("applicationConfiguration");

        UpgradeModel request =
            inputParameters.get(JobMapKeys.REQUEST.getKeyName(), UpgradeModel.class);
        AuthenticatedUserRequest user = inputParameters.get(
            JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

        //auth
        addStep(new MigrateDatabaseAuthStep(iamService, applicationConfiguration, user));
        //datarepo migrate
        addStep(new MigrateDataRepoDatabaseStep(migrate, migrateConfiguration, request, user));
        //stairway migrate
    }

}
