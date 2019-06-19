package bio.terra.flight.study.create;

import bio.terra.dao.StudyDao;
import bio.terra.metadata.Study;
import bio.terra.model.StudyJsonConversion;
import bio.terra.model.StudyRequestModel;
import bio.terra.pdao.PrimaryDataAccess;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.springframework.http.HttpStatus;

import java.util.UUID;

public class CreateStudyPrimaryDataStep implements Step {

    private final PrimaryDataAccess pdao;
    private final StudyDao studyDao;

    public CreateStudyPrimaryDataStep(PrimaryDataAccess pdao, StudyDao studyDao) {
        this.pdao = pdao;
        this.studyDao = studyDao;
    }

    Study getStudy(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        UUID studyId = workingMap.get("studyId", UUID.class);
        return studyDao.retrieve(studyId);
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Study study = getStudy(context);
        pdao.createStudy(study);
        FlightMap map = context.getWorkingMap();
        map.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.CREATED);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        pdao.deleteStudy(getStudy(context));
        return StepResult.getStepResultSuccess();
    }
}

