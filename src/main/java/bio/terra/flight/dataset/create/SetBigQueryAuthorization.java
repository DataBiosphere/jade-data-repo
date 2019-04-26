package bio.terra.flight.dataset.create;

import bio.terra.pdao.bigquery.BigQueryContainerInfo;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.google.cloud.bigquery.Acl;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;

import java.util.Collections;

public class SetBigQueryAuthorization implements Step {

    private BigQueryPdao bigQueryPdao;

    public SetBigQueryAuthorization(BigQueryPdao bigQueryPdao) {
        this.bigQueryPdao = bigQueryPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        BigQueryContainerInfo bqInfo = workingMap.get(
            JobMapKeys.BQ_DATASET_INFO.getKeyName(), BigQueryContainerInfo.class);

        // TODO: reveiwers: should these next 2 actions be in 2 different Steps? seems like overkill since there
        // really isn't an undo for them

        // set the view acls for the dataset on each of the studies
        bqInfo.getStudyToViewAcls().entrySet().stream().forEach(entry -> {
                DatasetInfo studyInfo = DatasetInfo.newBuilder(DatasetId.of(entry.getKey()))
                    .setAcl(entry.getValue()).build();
                bigQueryPdao.updateDataset(studyInfo);
            }
        );

        // set the reader group access on the dataset
        DatasetInfo datasetInfo = DatasetInfo.newBuilder(bqInfo.getBqDatasetId()).setAcl(
            Collections.singletonList(Acl.of(new Acl.Group(bqInfo.getReadersEmail()), Acl.Role.READER))).build();
        bigQueryPdao.updateDataset(datasetInfo);

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // if this step fails the next undo step will delete the dataset so i don't think we need to
        // remove the acl
        return StepResult.getStepResultSuccess();
    }


}
