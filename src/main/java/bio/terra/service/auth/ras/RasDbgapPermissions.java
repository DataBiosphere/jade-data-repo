package bio.terra.service.auth.ras;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record RasDbgapPermissions(String consent_group, String phs_id) {}
