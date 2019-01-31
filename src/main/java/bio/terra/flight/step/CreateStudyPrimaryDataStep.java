package bio.terra.flight.step;

import bio.terra.metadata.Study;
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

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        StudyRequestModel studyRequest = inputParameters.get("request", StudyRequestModel.class);
        Study newStudy = new Study(studyRequest);
        bigQueryPdao.createStudy(newStudy);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }
}

