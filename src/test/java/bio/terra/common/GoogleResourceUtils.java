package bio.terra.common;

import bio.terra.app.model.GoogleRegion;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import com.google.api.client.util.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class GoogleResourceUtils {
  @Autowired private BufferService bufferService;
  @Autowired private GoogleProjectService projectService;

  public GoogleProjectResource buildProjectResource(BillingProfileModel profile) throws Exception {
    String role = "roles/bigquery.jobUser";
    String stewardsGroupEmail = "group:JadeStewards-dev@dev.test.firecloud.org";
    List<String> stewardsGroupEmailList = Lists.newArrayList();
    stewardsGroupEmailList.add(stewardsGroupEmail);
    Map<String, List<String>> roleToStewardMap = new HashMap<>();
    roleToStewardMap.put(role, stewardsGroupEmailList);

    ResourceInfo resourceInfo = bufferService.handoutResource(false);

    // create project metadata
    return projectService.initializeGoogleProject(
        resourceInfo.getCloudResourceUid().getGoogleProjectUid().getProjectId(),
        profile,
        roleToStewardMap,
        GoogleRegion.DEFAULT_GOOGLE_REGION,
        Map.of("test-name", "bucket-resource-test"));
  }
}
