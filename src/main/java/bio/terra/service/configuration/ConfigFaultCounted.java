package bio.terra.service.configuration;

import bio.terra.app.controller.exception.ValidationException;
import bio.terra.model.ConfigFaultCountedModel;
import bio.terra.model.ConfigFaultModel;
import bio.terra.model.ConfigModel;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressFBWarnings(
    value = "DMI_RANDOM_USED_ONLY_ONCE",
    justification = "False positive introduced in 4.2.3, fixed in 4.4.2")
public class ConfigFaultCounted extends ConfigFault {
  private final Logger logger = LoggerFactory.getLogger(ConfigFaultCounted.class);

  private final ConfigFaultCountedModel originalCountedModel;

  // This state is synchronized because it is referenced in synchronized code and findbugs
  // wants it to be consistently synchronized everywhere...
  private ConfigFaultCountedModel countedModel;

  // Access to this state must be synchronized so that multiple threads do not mess it up.
  private boolean doneSkipping;
  private int skippedCounter;
  private boolean doneInserting;
  private int insertCounter;
  private Random random;
  private int every;
  private int everyCounter;
  private int testCounter;

  // -- Instance methods --
  ConfigFaultCounted(
      ConfigEnum configEnum,
      ConfigFaultModel.FaultTypeEnum faultType,
      boolean enabled,
      ConfigFaultCountedModel countedModel) {
    super(configEnum, faultType, enabled);
    this.originalCountedModel = countedModel;
    this.countedModel = countedModel;
    initCounters();
  }

  @Override
  synchronized ConfigModel get() {
    ConfigFaultModel faultCounted =
        new ConfigFaultModel()
            .faultType(ConfigFaultModel.FaultTypeEnum.COUNTED)
            .enabled(isEnabled())
            .counted(countedModel);

    return new ConfigModel()
        .configType(ConfigModel.ConfigTypeEnum.FAULT)
        .name(getName())
        .fault(faultCounted);
  }

  @Override
  void validate(ConfigModel configModel) {
    super.validate(configModel);
    if (configModel.getFault().getCounted() == null) {
      throw new ValidationException("Set of a counted fault requires a counted fault model");
    }
  }

  @Override
  synchronized void setFaultConfig(ConfigFaultModel faultModel) {
    countedModel = faultModel.getCounted();
    initCounters();
  }

  @Override
  synchronized void resetFaultConfig() {
    countedModel = originalCountedModel;
    initCounters();
  }

  @Override
  synchronized boolean testInsertFault() {
    if (!isEnabled()) {
      return false;
    }

    if (!doneSkipping) {
      skippedCounter++;
      if (skippedCounter >= countedModel.getSkipFor()) {
        doneSkipping = true;
      }
      return false;
    }

    if (!doneInserting) {
      testCounter++;
      switch (countedModel.getRateStyle()) {
        case RANDOM:
          if (random.nextInt(100) < countedModel.getRate()) {
            logger.debug("Fault: random insert - insert=" + insertCounter + " test=" + testCounter);
            return doInsert();
          }
          break;

        case FIXED:
          everyCounter++;
          logger.debug("Fault: fixed insert - every=" + every + " everyCounter=" + everyCounter);
          if (everyCounter >= every) {
            everyCounter = 0;
            return doInsert();
          }
          break;
      }
    }

    logger.info("Fault: NOT inserting fault " + getConfigEnum());
    return false;
  }

  private boolean doInsert() {
    if (countedModel.getInsert() < 0) {
      insertCounter++;
      return true;
    } else {
      if (insertCounter < countedModel.getInsert()) {
        insertCounter++;
        logger.info("Fault: inserting fault " + getConfigEnum());
        return true;
      } else {
        doneInserting = true;
        logger.debug(
            "Fault: done inserting "
                + getConfigEnum()
                + "; inserted "
                + insertCounter
                + " of "
                + testCounter
                + " tests");
      }
    }

    logger.info("Fault: NOT inserting fault " + getConfigEnum());
    return false;
  }

  private synchronized void initCounters() {
    doneSkipping = (countedModel.getSkipFor() == 0);
    skippedCounter = 0;
    doneInserting = false;
    insertCounter = 0;
    testCounter = 0;
    switch (countedModel.getRateStyle()) {
      case RANDOM:
        if (random == null) {
          random = new Random();
        }
        break;

      case FIXED:
        every = 100 / countedModel.getRate();
        everyCounter = 0;
        break;
      default:
        logger.error("Rate style is not one of the two defined cases: RANDOM or FIXED");
        break;
    }
  }
}
