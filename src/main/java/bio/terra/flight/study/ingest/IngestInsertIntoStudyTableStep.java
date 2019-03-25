package bio.terra.flight.study.ingest;

import bio.terra.dao.StudyDao;
import bio.terra.metadata.Study;
import bio.terra.metadata.Table;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.pdao.PdaoLoadStatistics;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class IngestInsertIntoStudyTableStep implements Step {
    private StudyDao studyDao;
    private BigQueryPdao bigQueryPdao;

    public IngestInsertIntoStudyTableStep(StudyDao studyDao, BigQueryPdao bigQueryPdao) {
        this.studyDao = studyDao;
        this.bigQueryPdao = bigQueryPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Study study = IngestUtils.getStudy(context, studyDao);
        Table targetTable = IngestUtils.getStudyTable(context, study);
        String stagingTableName = IngestUtils.getStagingTableName(context);
        bigQueryPdao.insertIntoStudyTable(study, targetTable, stagingTableName);

        IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(context);
        PdaoLoadStatistics loadStatistics = IngestUtils.getIngestStatistics(context);

        IngestResponseModel ingestResponse = new IngestResponseModel()
            .study(study.getName())
            .studyId(study.getId().toString())
            .table(ingestRequest.getTable())
            .path(ingestRequest.getPath())
            .loadTag(ingestRequest.getLoadTag())
            .badRowCount(loadStatistics.getBadRecords())
            .rowCount(loadStatistics.getRowCount());
        context.getWorkingMap().put(JobMapKeys.RESPONSE.getKeyName(), ingestResponse);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // The update will update row ids that are null, so it can be rerun on failure.
        return StepResult.getStepResultSuccess();
    }
}
