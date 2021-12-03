package bio.terra.service.snapshot.flight.export;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class SnapshotExportFlight extends Flight {

  public SnapshotExportFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    ApplicationContext appContext = (ApplicationContext) applicationContext;
    ResourceService resourceService = appContext.getBean(ResourceService.class);
    SnapshotService snapshotService = appContext.getBean(SnapshotService.class);
    BigQueryPdao bigQueryPdao = appContext.getBean(BigQueryPdao.class);
    GcsPdao gcsPdao = appContext.getBean(GcsPdao.class);
    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);
    ObjectMapper objectMapper = appConfig.objectMapper();

    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
    UUID snapshotId =
        UUID.fromString(inputParameters.get(JobMapKeys.SNAPSHOT_ID.getKeyName(), String.class));

    addStep(new SnapshotExportCreateBucketStep(resourceService, snapshotService, snapshotId));
    addStep(
        new SnapshotExportCreateParquetFilesStep(
            bigQueryPdao, gcsPdao, snapshotService, snapshotId));
    addStep(
        new SnapshotExportWriteManifestStep(
            snapshotId, snapshotService, gcsPdao, objectMapper, userReq));
    addStep(new SnapshotExportGrantPermissionsStep(gcsPdao, userReq));
  }
}
