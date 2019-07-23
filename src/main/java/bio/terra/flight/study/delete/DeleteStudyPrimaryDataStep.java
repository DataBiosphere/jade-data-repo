package bio.terra.flight.study.delete;

import bio.terra.dao.StudyDao;
import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.metadata.Study;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.springframework.http.HttpStatus;

import java.util.UUID;

public class DeleteStudyPrimaryDataStep implements Step {
    private BigQueryPdao bigQueryPdao;
    private GcsPdao gcsPdao;
    private FireStoreFileDao fileDao;
    private StudyDao studyDao;

    public DeleteStudyPrimaryDataStep(BigQueryPdao bigQueryPdao,
                                      GcsPdao gcsPdao,
                                      FireStoreFileDao fileDao,
                                      StudyDao studyDao) {
        this.bigQueryPdao = bigQueryPdao;
        this.gcsPdao = gcsPdao;
        this.fileDao = fileDao;
        this.studyDao = studyDao;
    }

    Study getStudy(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        UUID studyId = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), UUID.class);
        return studyDao.retrieve(studyId);
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Study study = getStudy(context);
        bigQueryPdao.deleteStudy(study);
        gcsPdao.deleteFilesFromStudy(study);
        fileDao.deleteFilesFromStudy(study);

        FlightMap map = context.getWorkingMap();
        map.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.NO_CONTENT);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // can't undo delete
        return StepResult.getStepResultSuccess();
    }
}
