package bio.terra.service.dataproject;

import bio.terra.metadata.FSFile;
import bio.terra.metadata.Snapshot;
import bio.terra.metadata.Dataset;
import bio.terra.resourcemanagement.service.google.GoogleResourceConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Primary
@Profile("!terra")
@Component
public class OneDataLocationSelector implements DataLocationSelector {

    private final GoogleResourceConfiguration resourceConfiguration;

    @Autowired
    public OneDataLocationSelector(GoogleResourceConfiguration resourceConfiguration) {
        this.resourceConfiguration = resourceConfiguration;
    }

    private String oneProject() {
        return resourceConfiguration.getProjectId() + "-data";
    }

    @Override
    public String projectIdForDataset(Dataset dataset) {
        return oneProject();
    }

    @Override
    public String projectIdForSnapshot(Snapshot snapshot) {
        return oneProject();
    }

    @Override
    public String projectIdForFile(FSFile fsFile) {
        return oneProject();
    }

    @Override
    public String bucketForFile(FSFile fsFile) {
        return oneProject() + "-bucket";
    }
}
