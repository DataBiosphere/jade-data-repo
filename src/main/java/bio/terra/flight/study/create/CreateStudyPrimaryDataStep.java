package bio.terra.flight.study.create;

import bio.terra.metadata.Study;
import bio.terra.model.StudyJsonConversion;
import bio.terra.model.StudyRequestModel;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.springframework.http.HttpStatus;

public class CreateStudyPrimaryDataStep implements Step {

    private BigQueryPdao bigQueryPdao;

    public CreateStudyPrimaryDataStep(BigQueryPdao bigQueryPdao) {
        this.bigQueryPdao = bigQueryPdao;
    }

    Study getStudy(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        StudyRequestModel studyRequest = inputParameters.get(JobMapKeys.REQUEST, StudyRequestModel.class);
        return StudyJsonConversion.studyRequestToStudy(studyRequest);
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Study study = getStudy(context);
        bigQueryPdao.createStudy(study);
        FlightMap map = context.getWorkingMap();
        map.put(JobMapKeys.STATUS_CODE, HttpStatus.CREATED);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        bigQueryPdao.deleteStudy(getStudy(context));
        return StepResult.getStepResultSuccess();
    }
}

