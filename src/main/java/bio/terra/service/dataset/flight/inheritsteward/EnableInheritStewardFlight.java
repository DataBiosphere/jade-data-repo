package bio.terra.service.dataset.flight.inheritsteward;

import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class EnableInheritStewardFlight extends Flight {
  public EnableInheritStewardFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    // Get the required DAOs and services to pass into the steps
    ApplicationContext appContext = (ApplicationContext) applicationContext;
    DatasetDao datasetDao = appContext.getBean(DatasetDao.class);
    ResourceService resourceService = appContext.getBean(ResourceService.class);
    SnapshotService snapshotService = appContext.getBean(SnapshotService.class);
    BigQuerySnapshotPdao bigQuerySnapshotPdao = appContext.getBean(BigQuerySnapshotPdao.class);

    // Get the input parameters
    UUID datasetId = inputParameters.get(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), UUID.class);
    String custodianEmail =
        inputParameters.get(JobMapKeys.CUSTODIAN_EMAIL.getKeyName(), String.class);

    boolean inheritSteward = true;
    addStep(new InheritStewardSetFlagStep(datasetDao, datasetId, inheritSteward));
    addStep(new GetSnapshotGoogleProjectIdsStep(snapshotService, datasetId));
    addStep(new SetAuthBqJobUserStep(resourceService, custodianEmail, inheritSteward));
    addStep(
        new SetAuthTabluarAclStep(
            bigQuerySnapshotPdao, snapshotService, custodianEmail, inheritSteward));
  }
}
