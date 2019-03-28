package bio.terra.flight.study.ingest;

import bio.terra.dao.StudyDao;
import bio.terra.metadata.Study;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class IngestRowIdsStep implements Step {
    private StudyDao studyDao;
    private BigQueryPdao bigQueryPdao;

    public IngestRowIdsStep(StudyDao studyDao, BigQueryPdao bigQueryPdao) {
        this.studyDao = studyDao;
        this.bigQueryPdao = bigQueryPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Study study = IngestUtils.getStudy(context, studyDao);
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
