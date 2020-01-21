package bio.terra.service.snapshot.flight.create;

import bio.terra.common.exception.UnauthorizedException;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.create.CreateDatasetAuthzResource;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotRequestContainer;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class AuthorizeSnapshot implements Step {
    private IamService sam;
    private BigQueryPdao bigQueryPdao;
    private FireStoreDependencyDao fireStoreDao;
    private SnapshotService snapshotService;
    private GcsPdao gcsPdao;
    private DatasetService datasetService;
    private AuthenticatedUserRequest userReq;
    private SnapshotRequestContainer snapshotRequestContainer;
    private static Logger logger = LoggerFactory.getLogger(CreateDatasetAuthzResource.class);

    public AuthorizeSnapshot(BigQueryPdao bigQueryPdao,
                             IamService sam,
                             FireStoreDependencyDao fireStoreDao,
                             SnapshotService snapshotService,
                             GcsPdao gcsPdao,
                             DatasetService datasetService,
                             SnapshotRequestContainer snapshotRequestContainer,
                             AuthenticatedUserRequest userReq) {
        this.bigQueryPdao = bigQueryPdao;
        this.sam = sam;
        this.fireStoreDao = fireStoreDao;
        this.snapshotService = snapshotService;
        this.gcsPdao = gcsPdao;
        this.datasetService = datasetService;
        this.snapshotRequestContainer = snapshotRequestContainer;
        this.userReq = userReq;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);
        Snapshot snapshot = snapshotService.retrieve(snapshotId);

        // This returns the policy email created by Google to correspond to the readers list in SAM
        String readersPolicyEmail = sam.createSnapshotResource(
            userReq, snapshotId, snapshotRequestContainer.getReaders());
        bigQueryPdao.addReaderGroupToSnapshot(snapshot, readersPolicyEmail);

        // Each dataset may keep its dependencies in its own scope. Therefore,
        // we have to iterate through the datasets in the snapshot and ask each one
        // to give us its list of file ids. Then we set acls on the files for that
        // dataset used by the snapshot.
        for (SnapshotSource snapshotSource : snapshot.getSnapshotSources()) {
            String datasetId = snapshotSource.getDataset().getId().toString();
            Dataset dataset = datasetService.retrieve(UUID.fromString(datasetId));
            List<String> fileIds = fireStoreDao.getDatasetSnapshotFileIds(dataset, snapshotId.toString());
            gcsPdao.setAclOnFiles(dataset, fileIds, readersPolicyEmail);
        }
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);
        try {
            sam.deleteSnapshotResource(userReq, snapshotId);
            // We do not need to remove the ACL from the files or BigQuery. It disappears
            // when SAM deletes the ACL. How 'bout that!
        } catch (UnauthorizedException ex) {
            // suppress exception
            logger.error("NEEDS CLEANUP: delete sam resource for snapshot " + snapshotId.toString());
            logger.warn(ex.getMessage());
        }
        return StepResult.getStepResultSuccess();
    }
}
