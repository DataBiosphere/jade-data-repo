package bio.terra.service.rawls;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record WorkspaceDetails(String workspaceId, String namespace, String name) {}
