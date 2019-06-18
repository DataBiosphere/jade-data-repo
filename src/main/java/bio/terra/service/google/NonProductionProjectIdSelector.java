package bio.terra.service.google;

import bio.terra.metadata.google.DataProjectRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class NonProductionProjectIdSelector implements GoogleProjectIdSelector {

    private final GoogleResourceConfiguration resourceConfiguration;

    @Autowired
    public NonProductionProjectIdSelector(GoogleResourceConfiguration resourceConfiguration) {
        this.resourceConfiguration = resourceConfiguration;
    }

    @Override
    public String projectId(DataProjectRequest dataProjectRequest) {
        return resourceConfiguration.getProjectId() + "-data";
    }
}
