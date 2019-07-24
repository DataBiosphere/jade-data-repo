package bio.terra.flight.study.ingest;

import bio.terra.metadata.Study;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.StudyService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestCleanupStep implements Step {
    private Logger logger = LoggerFactory.getLogger("bio.terra.study.ingest");

    private final StudyService studyService;
    private final BigQueryPdao bigQueryPdao;

    public IngestCleanupStep(StudyService studyService, BigQueryPdao bigQueryPdao) {
        this.studyService = studyService;
        this.bigQueryPdao = bigQueryPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        // We do not want to fail the insert because we fail to cleanup the staging table.
        // We log the failure and move on.
        String stagingTableName = "<unknown>";
        try {
            Study study = IngestUtils.getStudy(context, studyService);
            stagingTableName = IngestUtils.getStagingTableName(context);
            bigQueryPdao.deleteStudyTable(study, stagingTableName);
        } catch (Exception ex) {
            logger.error("Failure deleting ingest staging table: " + stagingTableName, ex);
        }
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }
}
