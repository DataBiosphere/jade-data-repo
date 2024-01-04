package bio.terra.service.duos;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record DuosDataset(Integer dataSetId) {}
