package bio.terra.service.dataproject;

import bio.terra.metadata.Dataset;
import bio.terra.metadata.Snapshot;
import bio.terra.resourcemanagement.service.google.GoogleResourceConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Profile("!terra")
@Component
public class OneDataProjectIdSelector implements DataProjectIdSelector {

    private final GoogleResourceConfiguration resourceConfiguration;

    @Autowired
    public OneDataProjectIdSelector(GoogleResourceConfiguration resourceConfiguration) {
        this.resourceConfiguration = resourceConfiguration;
    }

    @Override
    public String projectIdForDataset(Dataset dataset) {
        return resourceConfiguration.getProjectId() + "-data";
    }

    @Override
    public String projectIdForSnapshot(Snapshot snapshot) {
        return resourceConfiguration.getProjectId() + "-data";
    }
}
