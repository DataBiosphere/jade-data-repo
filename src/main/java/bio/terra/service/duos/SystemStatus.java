package bio.terra.service.duos;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record SystemStatus(
    Boolean ok, Boolean degraded, Map<String, SystemStatusSystems> systems) {}
