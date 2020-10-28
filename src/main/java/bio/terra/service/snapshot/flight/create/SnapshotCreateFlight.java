package bio.terra.service.snapshot.flight.create;

import bio.terra.app.logging.PerformanceLogger;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.AuthorizeBillingProfileUseStep;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.exception.InvalidSnapshotException;
import bio.terra.service.snapshot.flight.UnlockSnapshotStep;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.RetryRuleExponentialBackoff;
import org.springframework.context.ApplicationContext;

public class SnapshotCreateFlight extends Flight {

    public SnapshotCreateFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        // get the required objects to pass into the steps
        ApplicationContext appContext = (ApplicationContext) applicationContext;
        SnapshotDao snapshotDao = (SnapshotDao)appContext.getBean("snapshotDao");
        SnapshotService snapshotService = (SnapshotService)appContext.getBean("snapshotService");
        BigQueryPdao bigQueryPdao = (BigQueryPdao)appContext.getBean("bigQueryPdao");
        FireStoreDependencyDao dependencyDao = (FireStoreDependencyDao)appContext.getBean("fireStoreDependencyDao");
        FireStoreDao fileDao = (FireStoreDao)appContext.getBean("fireStoreDao");
        IamService iamClient = (IamService)appContext.getBean("iamService");
        GcsPdao gcsPdao = (GcsPdao) appContext.getBean("gcsPdao");
        DatasetService datasetService = (DatasetService) appContext.getBean("datasetService");
        ConfigurationService configService = (ConfigurationService) appContext.getBean("configurationService");
        ResourceService resourceService = (ResourceService) appContext.getBean("resourceService");
        PerformanceLogger performanceLogger = (PerformanceLogger) appContext.getBean("performanceLogger");
        ProfileService profileService = (ProfileService) appContext.getBean("profileService");

        SnapshotRequestModel snapshotReq = inputParameters.get(
            JobMapKeys.REQUEST.getKeyName(), SnapshotRequestModel.class);
        String snapshotName = snapshotReq.getName();

        AuthenticatedUserRequest userReq = inputParameters.get(
            JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

        // Make sure this user is allowed to use the billing profile and that the underlying
        // billing information remains valid.
        addStep(new AuthorizeBillingProfileUseStep(profileService, snapshotReq.getProfileId(), userReq));

        // Get or create the project where the snapshot resources will be created
        addStep(new CreateSnapshotGetOrCreateProjectStep(resourceService, snapshotReq));

        // create the snapshot metadata object in postgres and lock it
        addStep(new CreateSnapshotMetadataStep(snapshotDao, snapshotService, snapshotReq));

        // Make the big query dataset with views and populate row id filtering tables.
        // Depending on the type of snapshot, the primary data step will differ:
        // TODO: this assumes single-dataset snapshots, will need to add a loop for multiple
        switch (snapshotReq.getContents().get(0).getMode()) {
            case BYASSET:
                addStep(new CreateSnapshotValidateAssetStep(datasetService, snapshotService, snapshotReq));
                addStep(new CreateSnapshotPrimaryDataAssetStep(
                    bigQueryPdao, snapshotDao, snapshotService, snapshotReq));
                break;
            case BYFULLVIEW:
                addStep(new CreateSnapshotPrimaryDataFullViewStep(
                    bigQueryPdao, datasetService, snapshotDao, snapshotService, snapshotReq));
                break;
            case BYQUERY:
                addStep(new CreateSnapshotValidateQueryStep(datasetService, snapshotReq));
                addStep(new CreateSnapshotPrimaryDataQueryStep(
                    bigQueryPdao, datasetService, snapshotService, snapshotDao, snapshotReq));
                break;
            case BYROWID:
                addStep(new CreateSnapshotPrimaryDataRowIdsStep(
                    bigQueryPdao, snapshotDao, snapshotService, snapshotReq));
                break;
            default:
                throw new InvalidSnapshotException("Snapshot does not have required mode information");
        }

        // compute the row counts for each of the snapshot tables and store in metadata
        addStep(new CountSnapshotTableRowsStep(bigQueryPdao, snapshotDao, snapshotReq));

        // Create the IAM resource and readers for the snapshot
        // The IAM code contains retries, so we don't make a retry rule here.
        addStep(new SnapshotAuthzIamStep(iamClient, snapshotService, snapshotReq, userReq));

        // Make the firestore file system for the snapshot
        addStep(new CreateSnapshotFireStoreDataStep(
            bigQueryPdao, snapshotService, dependencyDao, datasetService, snapshotReq, fileDao, performanceLogger));

        // Calculate checksums and sizes for all directories in the snapshot
        addStep(new CreateSnapshotFireStoreComputeStep(snapshotService, snapshotReq, fileDao));

        // Google says that ACL change propagation happens in a few seconds, but can take 5-7 minutes. The max
        // operation timeout is generous.
        RetryRuleExponentialBackoff pdaoAclRetryRule =
            new RetryRuleExponentialBackoff(2, 30, 600);

        // Apply the IAM readers to the BQ dataset
        addStep(new SnapshotAuthzTabularAclStep(bigQueryPdao, snapshotService, configService), pdaoAclRetryRule);

        // Apply the IAM readers to the GCS files
        addStep(new SnapshotAuthzFileAclStep(
            dependencyDao,
            snapshotService,
            gcsPdao,
            datasetService,
            configService), pdaoAclRetryRule);

        addStep(new SnapshotAuthzBqJobUserStep(
            snapshotService,
            resourceService,
            snapshotName));

       // unlock the snapshot metadata row
        addStep(new UnlockSnapshotStep(snapshotDao, null));
    }
}
