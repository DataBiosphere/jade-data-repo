package bio.terra.service.duos;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.annotations.VisibleForTesting;

@VisibleForTesting
@JsonIgnoreProperties(ignoreUnknown = true)
public record DuosDataset(Integer dataSetId) {}
