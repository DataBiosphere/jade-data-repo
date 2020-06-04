package bio.terra.service.job;

import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightFactory;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.exception.MakeFlightException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

@Component
public class JobFlightFactory implements FlightFactory {
    private final static Logger logger = LoggerFactory.getLogger(JobFlightFactory.class);

    @Override
    public Flight makeFlight(Class<? extends Flight> flightClass, FlightMap inputParameters, Object context) {
        try {
            // Find the flightClass constructor that takes the input parameter map and
            // use it to make the flight.
            Constructor constructor = flightClass.getConstructor(FlightMap.class, Object.class);
            Flight flight = (Flight) constructor.newInstance(inputParameters, context);
            logger.info("Constructed flight object has class loader " + flight.getClass().getClassLoader());
            return flight;
        } catch (InvocationTargetException
            | NoSuchMethodException
            | InstantiationException
            | IllegalAccessException ex) {
            throw new MakeFlightException("Failed to make a flight from class '" + flightClass + "'", ex);
        }
    }

    @Override
    public Flight makeFlightFromName(String className, FlightMap inputParameters, Object context) {
        try {
            Class<?> someClass = Class.forName(className);
            logger.info("Loaded class " + className + " It has class loader " + someClass.getClassLoader());
            if (Flight.class.isAssignableFrom(someClass)) {
                Class<? extends Flight> flightClass = (Class<? extends Flight>) someClass;
                return makeFlight(flightClass, inputParameters, context);
            }
            // Error case
            throw new MakeFlightException(
                "Failed to make a flight from class name '"
                    + className
                    + "' - it is not a subclass of Flight");

        } catch (ClassNotFoundException ex) {
            throw new MakeFlightException(
                "Failed to make a flight from class name '" + className + "'", ex);
        }
    }
}
