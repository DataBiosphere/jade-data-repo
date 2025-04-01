package bio.terra.service.rawls;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record RawlsBillingProjectResponse(String projectName) { }
