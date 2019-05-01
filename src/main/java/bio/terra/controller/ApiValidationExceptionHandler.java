package bio.terra.controller;


import bio.terra.model.ErrorModel;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.util.List;

import static java.util.stream.Collectors.toList;

@ControllerAdvice
public class ApiValidationExceptionHandler extends ResponseEntityExceptionHandler {

    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(
        MethodArgumentNotValidException ex,
        HttpHeaders headers,
        HttpStatus status,
        WebRequest request
    ) {
        BindingResult bindingResult = ex.getBindingResult();

        List<String> errorDetails = bindingResult
            .getFieldErrors()
            .stream()
            .map(err -> err.getCode() + " error on '" + err.getObjectName() + "': " + err.getDefaultMessage())
            .collect(toList());

        ErrorModel errorModel = new ErrorModel()
            .message("Validation errors - see error details")
            .errorDetail(errorDetails);

        return new ResponseEntity<>(errorModel, HttpStatus.BAD_REQUEST);
    }
}

