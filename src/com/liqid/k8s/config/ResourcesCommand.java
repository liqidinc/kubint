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

class ResourcesCommand extends Command {

    ResourcesCommand(
        final Logger logger,
        final String proxyURL,
        final Integer timeoutInSeconds
    ) {
        super(logger, proxyURL, false, timeoutInSeconds);
    }

    public ResourcesCommand setLiqidAddress(final String value) { _liqidAddress = value; return this; }
    public ResourcesCommand setLiqidPassword(final String value) { _liqidPassword = value; return this; }
    public ResourcesCommand setLiqidUsername(final String value) { _liqidUsername = value; return this; }

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

        if (!initLiqidClient()) {
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        getLiqidInventory();
        displayDevices(null);
        displayMachines(null);

        _logger.trace("Exiting %s true", fn);
        return true;
    }
}
