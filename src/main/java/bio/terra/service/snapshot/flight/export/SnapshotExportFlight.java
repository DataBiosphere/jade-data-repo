package bio.terra.service.snapshot.flight.export;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.tabulardata.google.bigquery.BigQueryExportPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Objects;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class SnapshotExportFlight extends Flight {

  public SnapshotExportFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    ApplicationContext appContext = (ApplicationContext) applicationContext;
    ResourceService resourceService = appContext.getBean(ResourceService.class);
    SnapshotService snapshotService = appContext.getBean(SnapshotService.class);
    BigQueryExportPdao bigQueryExportPdao = appContext.getBean(BigQueryExportPdao.class);
    GcsPdao gcsPdao = appContext.getBean(GcsPdao.class);
    FireStoreDao fireStoreDao = appContext.getBean(FireStoreDao.class);
    ApplicationConfiguration appConfig = appContext.getBean(ApplicationConfiguration.class);
    ObjectMapper objectMapper = appConfig.objectMapper();

    AuthenticatedUserRequest userReq =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);
    UUID snapshotId =
        UUID.fromString(inputParameters.get(JobMapKeys.SNAPSHOT_ID.getKeyName(), String.class));

    boolean validatePrimaryKeyUniqueness =
        Objects.requireNonNullElse(
            inputParameters.get(ExportMapKeys.EXPORT_VALIDATE_PK_UNIQUENESS, Boolean.class), true);

    if (validatePrimaryKeyUniqueness) {
      addStep(new SnapshotExportValidatePrimaryKeysStep(snapshotService, snapshotId));
    }

    addStep(new SnapshotExportCreateBucketStep(resourceService, snapshotService, snapshotId));

    boolean exportGsPaths =
        Objects.requireNonNullElse(
            inputParameters.get(ExportMapKeys.EXPORT_GSPATHS, Boolean.class), false);
    if (exportGsPaths) {
      addStep(
          new SnapshotExportDumpFirestoreStep(
              snapshotService, fireStoreDao, gcsPdao, snapshotId, objectMapper));
      addStep(
          new SnapshotExportLoadMappingTableStep(snapshotId, snapshotService, bigQueryExportPdao));
    }
    addStep(
        new SnapshotExportCreateParquetFilesStep(
            bigQueryExportPdao, gcsPdao, snapshotService, snapshotId, exportGsPaths));
    addStep(
        new SnapshotExportWriteManifestStep(
            snapshotId,
            snapshotService,
            gcsPdao,
            objectMapper,
            userReq,
            validatePrimaryKeyUniqueness));
    addStep(new SnapshotExportGrantPermissionsStep(gcsPdao, userReq));
    if (exportGsPaths) {
      addStep(
          new CleanUpExportGsPathsStep(bigQueryExportPdao, gcsPdao, snapshotService, snapshotId));
    }
  }
}
