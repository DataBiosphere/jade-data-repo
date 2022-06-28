package bio.terra.service.auth.ras;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record RASDbgapPermissions(String consent_group, String phs_id) {}
