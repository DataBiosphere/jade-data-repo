package bio.terra.app.controller;

import bio.terra.common.exception.BadRequestException;
import bio.terra.common.exception.DataRepoException;
import bio.terra.common.exception.InternalServerErrorException;
import bio.terra.common.exception.NotFoundException;
import bio.terra.common.exception.NotImplementedException;
import bio.terra.common.exception.UnauthorizedException;
import bio.terra.model.ErrorModel;
import bio.terra.service.iam.sam.SamIam;
import bio.terra.service.job.exception.JobResponseException;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.NoHandlerFoundException;

import java.util.Collections;
import java.util.List;

@RestControllerAdvice
public class GlobalExceptionHandler {
    private final Logger logger =
        LoggerFactory.getLogger("bio.terra.controller.exception.GlobalExceptionHandler");

    // -- data repository base exceptions --
    @ExceptionHandler(NotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ErrorModel notFoundHandler(DataRepoException ex) {
        return buildErrorModel(ex, ex.getErrorDetails());
    }

    @ExceptionHandler(BadRequestException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorModel badRequestHandler(DataRepoException ex) {
        return buildErrorModel(ex, ex.getErrorDetails());
    }

    @ExceptionHandler(InternalServerErrorException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ErrorModel internalServerErrorHandler(DataRepoException ex) {
        return buildErrorModel(ex, ex.getErrorDetails());
    }

    @ExceptionHandler(NotImplementedException.class)
    @ResponseStatus(HttpStatus.NOT_IMPLEMENTED)
    public ErrorModel notImplementedHandler(DataRepoException ex) {
        return buildErrorModel(ex, ex.getErrorDetails());
    }

    // -- exceptions from validations - we don't control the exception raised --
    @ExceptionHandler({MethodArgumentNotValidException.class,
        IllegalArgumentException.class,
        NoHandlerFoundException.class})
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorModel validationExceptionHandler(Exception ex) {
        return buildErrorModel(ex);
    }

    // -- auth errors from sam
    @ExceptionHandler(UnauthorizedException.class)
    @ResponseStatus(HttpStatus.UNAUTHORIZED)
    public ErrorModel samAthorizationException(UnauthorizedException ex) {
        return buildErrorModel(ex, Collections.emptyList());
    }

    // -- job response exception -- we use the JobResponseException to wrap non-runtime exceptions
    // returned from flights. So at this level, we catch the JobResponseException and retrieve the
    // original exception from inside it and use that to construct the error model.
    @ExceptionHandler(JobResponseException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ErrorModel jobResponseExceptionHandler(Exception ex) {
        Throwable nestedException = ex.getCause();
        return buildErrorModel(nestedException);
    }

    @ExceptionHandler(ApiException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ErrorModel samApiExceptionHandler(ApiException ex) {
        // All SAM ApiExceptions should be caught inside the service/iam package and converted to a DataRepo exception
        // there. If any ApiException makes it up to this level, then it's unexpected. We can still do the conversion,
        // but want to add in a logging message that there's an escaped SAM ApiException somewhere.
        logger.error("SAM ApiException caught outside the service/iam package", ex);
        DataRepoException drex = SamIam.convertSAMExToDataRepoEx(ex);
        return buildErrorModel(drex);
    }

    // -- catchall - log so we can understand what we have missed in the handlers above
    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ErrorModel catchallHandler(Exception ex) {
        logger.error("Exception caught by catchall hander", ex);
        return buildErrorModel(ex);
    }

    // This method takes throwable so it can be shared by the JobResponseException handler:
    // the type returned from getCause() is a Throwable.
    // This error handler logs the complete error list so we can debug the underlying causes
    // of errors. We do not want to return that to the client, but need it for our own debugging.
    private ErrorModel buildErrorModel(Throwable ex) {
        return buildErrorModel(ex, null);
    }

    private ErrorModel buildErrorModel(Throwable ex, List<String> errorDetail) {
        logger.error("Global exception handler: catch stack", ex);
        for (Throwable cause = ex; cause != null; cause = cause.getCause()) {
            logger.error("   cause: " + cause.toString());
        }
        return new ErrorModel().message(ex.getMessage()).errorDetail(errorDetail);
    }
}
