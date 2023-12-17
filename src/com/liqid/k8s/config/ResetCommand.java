/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.config;

import com.bearsnake.k8sclient.K8SHTTPError;
import com.bearsnake.k8sclient.K8SJSONError;
import com.bearsnake.k8sclient.K8SRequestError;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.Command;
import com.liqid.k8s.exceptions.ConfigurationDataException;
import com.liqid.k8s.exceptions.ConfigurationException;
import com.liqid.sdk.LiqidException;

import static com.liqid.k8s.config.CommandType.RESET;

class ResetCommand extends Command {

    ResetCommand(
        final Logger logger,
        final String proxyURL,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, proxyURL, force, timeoutInSeconds);
    }

    ResetCommand setLiqidAddress(final String value) { _liqidAddress = value; return this; }
    ResetCommand setLiqidPassword(final String value) { _liqidPassword = value; return this; }
    ResetCommand setLiqidUsername(final String value) { _liqidUsername = value; return this; }

    @Override
    public boolean process(
    ) throws ConfigurationException,
             ConfigurationDataException,
             K8SHTTPError,
             K8SJSONError,
             K8SRequestError,
             LiqidException {
        var fn = RESET.getToken() + ":process";
        _logger.trace("Entering %s", fn);

        if (!_force) {
            System.err.println("ERROR: -f,--force must be set to run this command");
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        if (!initK8sClient()) {
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        if (!initLiqidClient()) {
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        // TODO clear annotations, configmaps, secrets

        // Clear Liqid configuration
        System.out.println("Deleting all Liqid groups...");
        _liqidClient.clearGroups();

        _logger.trace("Exiting %s true", fn);
        return true;
    }
}
