package bio.terra.service.duos;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record DuosDatasetApprovedUser(String email) {}
