package bio.terra.service.google;

import bio.terra.metadata.google.DataProjectRequest;

public interface GoogleProjectIdSelector {

   String projectId(DataProjectRequest dataProjectRequest);

}
