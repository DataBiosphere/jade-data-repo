package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class CreateSnapshotPrimaryDataFullViewStep implements Step {

    private BigQueryPdao bigQueryPdao;
    private DatasetService datasetservice;
    private SnapshotDao snapshotDao;
    private SnapshotService snapshotService;
    private SnapshotRequestModel snapshotReq;

    public CreateSnapshotPrimaryDataFullViewStep(BigQueryPdao bigQueryPdao,
                                                 DatasetService datasetservice,
                                                 SnapshotDao snapshotDao,
                                                 SnapshotService snapshotService,
                                                 SnapshotRequestModel snapshotReq) {
        this.bigQueryPdao = bigQueryPdao;
        this.datasetservice = datasetservice;
        this.snapshotDao = snapshotDao;
        this.snapshotService = snapshotService;
        this.snapshotReq = snapshotReq;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        /*
         * from the dataset tables, we will need to get the table's live views
         */
        SnapshotRequestContentsModel contentsModel = snapshotReq.getContents().get(0);
        Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
        Dataset dataset = datasetservice.retrieveByName(contentsModel.getDatasetName());
        bigQueryPdao.createSnapshotWithLiveViews(snapshot, dataset);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        snapshotService.undoCreateSnapshot(snapshotReq.getName());
        return StepResult.getStepResultSuccess();
    }

}

