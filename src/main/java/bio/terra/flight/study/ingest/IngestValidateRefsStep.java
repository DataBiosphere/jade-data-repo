package bio.terra.flight.study.ingest;

import bio.terra.dao.StudyDao;
import bio.terra.filesystem.FileDao;
import bio.terra.flight.exception.InvalidFileRefException;
import bio.terra.metadata.Column;
import bio.terra.metadata.FSObject;
import bio.terra.metadata.Study;
import bio.terra.metadata.Table;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class IngestValidateRefsStep implements Step {
    private static int MAX_ERROR_REF_IDS = 20;

    private StudyDao studyDao;
    private BigQueryPdao bigQueryPdao;
    private FileDao fileDao;

    public IngestValidateRefsStep(StudyDao studyDao, BigQueryPdao bigQueryPdao, FileDao fileDao) {
        this.studyDao = studyDao;
        this.bigQueryPdao = bigQueryPdao;
        this.fileDao = fileDao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Study study = IngestUtils.getStudy(context, studyDao);
        Table table = IngestUtils.getStudyTable(context, study);
        String stagingTableName = IngestUtils.getStagingTableName(context);

        // For each fileref column, scan the staging table and build an array of file ids
        // Then probe the file system tables to validate that the file exists and is part
        // of this study. We check all ids and return one complete error.

        List<String> invalidRefIds = new ArrayList<>();
        for (Column column : table.getColumns()) {
            if (StringUtils.equalsIgnoreCase(column.getType(), "FILEREF")) {
                List<String> refIdArray = bigQueryPdao.getRefIds(study.getName(), stagingTableName, column.getName());
                List<String> badRefIds = fileDao.validateRefIds(study.getId(), refIdArray, FSObject.FSObjectType.FILE);
                if (badRefIds != null) {
                    invalidRefIds.addAll(badRefIds);
                }
            }
        }

        if (invalidRefIds.size() != 0) {
            // TODO: improve when we have multi-item error model
            String badRefIdMessage =
                invalidRefIds.stream().limit(MAX_ERROR_REF_IDS).collect(Collectors.joining(", "));
            throw new InvalidFileRefException("Invalid file ids found during ingest (up to first " +
                MAX_ERROR_REF_IDS + "shown): " + badRefIdMessage);
        }

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // The update will update row ids that are null, so it can be restarted on failure.
        return StepResult.getStepResultSuccess();
    }
}
