package bio.terra.integration;

import bio.terra.model.ErrorModel;

/**
 * Specialization of ObjectOrErrorResponse for ErrorModel
 */
public class DataRepoResponse<T> extends ObjectOrErrorResponse<ErrorModel, T> {
}
