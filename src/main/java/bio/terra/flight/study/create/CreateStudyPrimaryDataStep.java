package bio.terra.flight.study.create;

import bio.terra.metadata.Study;
import bio.terra.model.StudyJsonConversion;
import bio.terra.model.StudyRequestModel;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class CreateStudyPrimaryDataStep implements Step {

    private BigQueryPdao bigQueryPdao;

    public CreateStudyPrimaryDataStep(BigQueryPdao bigQueryPdao) {
        this.bigQueryPdao = bigQueryPdao;
    }

    Study getStudy(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        StudyRequestModel studyRequest = inputParameters.get("request", StudyRequestModel.class);
        return StudyJsonConversion.studyRequestToStudy(studyRequest);
    }

    @Override
    public StepResult doStep(FlightContext context) {
        bigQueryPdao.createStudy(getStudy(context));
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        bigQueryPdao.deleteStudy(getStudy(context));
        return StepResult.getStepResultSuccess();
    }
}

