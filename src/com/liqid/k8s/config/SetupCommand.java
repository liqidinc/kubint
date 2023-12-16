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

import static com.liqid.k8s.config.CommandType.CLEANUP;

class SetupCommand extends Command {

    SetupCommand(
        final Logger logger,
        final String proxyURL,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, proxyURL, force, timeoutInSeconds);
    }

    public SetupCommand setLiqidAddress(final String value) { _liqidAddress = value; return this; }
    public SetupCommand setLiqidGroupName(final String value) { _liqidGroupName = value; return this; }
    public SetupCommand setLiqidPassword(final String value) { _liqidPassword = value; return this; }
    public SetupCommand setLiqidUsername(final String value) { _liqidUsername = value; return this; }

    public boolean checkConfiguration() {
        var fn = "checkConfiguration";
        _logger.trace("Entering %s", fn);

        // TODO make sure the Liqid config does not conflict with the setup instructions

        _logger.trace("Exiting %s true", fn);
        return true;
    }

    public boolean doConfiguration() throws LiqidException {
        var fn = "doConfiguration";
        _logger.trace("Entering %s", fn);

        if (!_groupsByName.containsKey(_liqidGroupName)) {
            _liqidClient.createGroup(_liqidGroupName);
        }

        // TODO

        _logger.trace("Exiting %s true", fn);
        return true;
    }

    @Override
    public boolean process(
    ) throws ConfigurationException,
             ConfigurationDataException,
             K8SHTTPError,
             K8SJSONError,
             K8SRequestError,
             LiqidException {
        var fn = CLEANUP.getToken() + ":process";
        _logger.trace("Entering %s", fn);

        if (!initK8sClient()) {
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        if (!getLiqidLinkage()) {
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        if (!initLiqidClient()) {
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        getLiqidInventory();
        if (!checkConfiguration() && !_force) {
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        if (!doConfiguration()) {
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        _logger.trace("Exiting %s true", fn);
        return true;
    }
}
