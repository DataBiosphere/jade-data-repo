package bio.terra.flight.study.ingest;

import bio.terra.dao.StudyDao;
import bio.terra.metadata.Study;
import bio.terra.metadata.Table;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class IngestCreateStagingTableStep implements Step {
    private StudyDao studyDao;
    private BigQueryPdao bigQueryPdao;

    public IngestCreateStagingTableStep(StudyDao studyDao, BigQueryPdao bigQueryPdao) {
        this.studyDao = studyDao;
        this.bigQueryPdao = bigQueryPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Study study = IngestUtils.getStudy(context, studyDao);
        Table targetTable = IngestUtils.getStudyTable(context, study);

        String stagingTableName = IngestUtils.getStagingTableName(context);
        bigQueryPdao.createStagingTable(study.getName(), targetTable, stagingTableName);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        IngestUtils.deleteStagingTable(context, bigQueryPdao);
        return StepResult.getStepResultSuccess();
    }
}
