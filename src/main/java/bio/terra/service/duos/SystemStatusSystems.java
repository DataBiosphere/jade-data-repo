package bio.terra.service.duos;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record SystemStatusSystems(Boolean healthy, String message, Object error, Object details) {}
