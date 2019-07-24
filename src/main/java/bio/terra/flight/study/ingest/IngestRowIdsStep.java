package bio.terra.flight.study.ingest;

import bio.terra.metadata.Study;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.StudyService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class IngestRowIdsStep implements Step {
    private StudyService studyService;
    private BigQueryPdao bigQueryPdao;

    public IngestRowIdsStep(StudyService studyService, BigQueryPdao bigQueryPdao) {
        this.studyService = studyService;
        this.bigQueryPdao = bigQueryPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Study study = IngestUtils.getStudy(context, studyService);
        String stagingTableName = IngestUtils.getStagingTableName(context);
        bigQueryPdao.addRowIdsToStagingTable(study, stagingTableName);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // The update will update row ids that are null, so it can be restarted on failure.
        return StepResult.getStepResultSuccess();
    }
}
