package bio.terra.integration;

import bio.terra.model.DRSError;

/**
 * Specialization of ObjectOrErrorResponse for ErrorModel
 */
public class DrsResponse<T> extends ObjectOrErrorResponse<DRSError, T> {

}
