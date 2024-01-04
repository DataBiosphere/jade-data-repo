package bio.terra.service.duos;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record DuosDatasetApprovedUsers(List<DuosDatasetApprovedUser> approvedUsers) {}
