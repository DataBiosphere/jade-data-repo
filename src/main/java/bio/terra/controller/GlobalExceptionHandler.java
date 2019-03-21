package bio.terra.controller;

import bio.terra.exception.BadRequestException;
import bio.terra.exception.InternalServerErrorException;
import bio.terra.exception.NotFoundException;
import bio.terra.model.ErrorModel;
import bio.terra.service.exception.JobResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.NoHandlerFoundException;

@RestControllerAdvice
public class GlobalExceptionHandler {
    private final Logger logger =
        LoggerFactory.getLogger("bio.terra.controller.exception.GlobalExceptionHandler");

    // -- data repository base exceptions --
    @ExceptionHandler(NotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ErrorModel notFoundHandler(Exception ex) {
        return buildErrorModel(ex);
    }

    @ExceptionHandler(BadRequestException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorModel badRequestHandler(Exception ex) {
        return buildErrorModel(ex);
    }

    @ExceptionHandler(InternalServerErrorException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ErrorModel internalServerErrorHandler(Exception ex) {
        return buildErrorModel(ex);
    }

    // -- exceptions from validations - we don't control the exception raised --
    @ExceptionHandler({MethodArgumentNotValidException.class,
        IllegalArgumentException.class,
        NoHandlerFoundException.class})
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorModel validationExceptionHandler(Exception ex) {
        return buildErrorModel(ex);
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

    // -- catchall - log so we can understand what we have missed in the handlers above
    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ErrorModel catchallHandler(Exception ex) {
        logger.error("Exception caught by catchall hander", ex);
        return buildErrorModel(ex);
    }

    // This method takes throwable so it can be shared by the JobResponseException handler:
    // the type returned from getCause() is a Throwable.
    private ErrorModel buildErrorModel(Throwable ex) {
        return new ErrorModel().message(ex.getMessage());
    }

}
