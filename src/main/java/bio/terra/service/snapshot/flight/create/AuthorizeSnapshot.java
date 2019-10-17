package bio.terra.service.snapshot.flight.create;

import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.common.exception.InternalServerErrorException;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.dataset.flight.create.CreateDatasetAuthzResource;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.iam.SamClientService;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.google.api.client.http.HttpStatusCodes;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class AuthorizeSnapshot implements Step {
    private SamClientService sam;
    private BigQueryPdao bigQueryPdao;
    private FireStoreDependencyDao fireStoreDao;
    private SnapshotService snapshotService;
    private GcsPdao gcsPdao;
    private DatasetService datasetService;
    private AuthenticatedUserRequest userReq;
    private SnapshotRequestModel snapshotReq;
    private static Logger logger = LoggerFactory.getLogger(CreateDatasetAuthzResource.class);

    public AuthorizeSnapshot(BigQueryPdao bigQueryPdao,
                             SamClientService sam,
                             FireStoreDependencyDao fireStoreDao,
                             SnapshotService snapshotService,
                             GcsPdao gcsPdao,
                             DatasetService datasetService,
                             SnapshotRequestModel snapshotReq,
                             AuthenticatedUserRequest userReq) {
        this.bigQueryPdao = bigQueryPdao;
        this.sam = sam;
        this.fireStoreDao = fireStoreDao;
        this.snapshotService = snapshotService;
        this.gcsPdao = gcsPdao;
        this.datasetService = datasetService;
        this.snapshotReq = snapshotReq;
        this.userReq = userReq;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);
        Snapshot snapshot = snapshotService.retrieveSnapshot(snapshotId);
        Optional<List<String>> readersList = Optional.ofNullable(snapshotReq.getReaders());
        try {
            // This returns the policy email created by Google to correspond to the readers list in SAM
            String readersPolicyEmail = sam.createSnapshotResource(userReq, snapshotId, readersList);
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
        } catch (ApiException ex) {
            throw new InternalServerErrorException("Couldn't add readers", ex);
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
        } catch (ApiException ex) {
            if (ex.getCode() == HttpStatusCodes.STATUS_CODE_UNAUTHORIZED) {
                // suppress exception
                logger.error("NEEDS CLEANUP: delete sam resource for snapshot " + snapshotId.toString());
                logger.warn(ex.getMessage());
            } else {
                throw new InternalServerErrorException(ex);
            }

        }
        return StepResult.getStepResultSuccess();
    }
}
