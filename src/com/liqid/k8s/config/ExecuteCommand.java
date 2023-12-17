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

import static com.liqid.k8s.config.CommandType.EXECUTE;
import static com.liqid.k8s.plan.LiqidInventory.getLiqidInventory;

class ExecuteCommand extends Command {

    private Boolean _noUpdate = false;

    ExecuteCommand(
        final Logger logger,
        final String proxyURL,
        final Integer timeoutInSeconds
    ) {
        super(logger, proxyURL, false, timeoutInSeconds);
    }

    ExecuteCommand setNoUpdate(final Boolean flag) { _noUpdate = flag; return this; }

    @Override
    public boolean process(
    ) throws ConfigurationException,
             ConfigurationDataException,
             K8SHTTPError,
             K8SJSONError,
             K8SRequestError,
             LiqidException {
        var fn = EXECUTE.getToken() + ":process";
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

        _liqidInventory = getLiqidInventory(_liqidClient, _logger);

        // TODO

        _logger.trace("Exiting %s true", fn);
        return true;
    }
}
