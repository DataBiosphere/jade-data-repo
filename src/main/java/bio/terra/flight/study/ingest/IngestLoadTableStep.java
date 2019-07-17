package bio.terra.flight.study.ingest;

import bio.terra.dao.StudyDao;
import bio.terra.metadata.Study;
import bio.terra.metadata.Table;
import bio.terra.model.IngestRequestModel;
import bio.terra.pdao.PdaoLoadStatistics;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class IngestLoadTableStep implements Step {
    private StudyDao studyDao;
    private BigQueryPdao bigQueryPdao;

    public IngestLoadTableStep(StudyDao studyDao, BigQueryPdao bigQueryPdao) {
        this.studyDao = studyDao;
        this.bigQueryPdao = bigQueryPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Study study = IngestUtils.getStudy(context, studyDao);
        Table targetTable = IngestUtils.getStudyTable(context, study);
        String stagingTableName = IngestUtils.getStagingTableName(context);
        IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(context);

        PdaoLoadStatistics ingestStatistics = bigQueryPdao.loadToStagingTable(
            study,
            targetTable,
            stagingTableName,
            ingestRequest);

        // Save away the stats in the working map. We will use some of them later
        // when we make the annotations. Others are returned on the ingest response.
        IngestUtils.putIngestStatistics(context, ingestStatistics);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        Study study = IngestUtils.getStudy(context, studyDao);
        String stagingTableName = IngestUtils.getStagingTableName(context);
        bigQueryPdao.deleteStudyTable(study, stagingTableName);
        return StepResult.getStepResultSuccess();
    }
}
