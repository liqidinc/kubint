/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.commands;

import com.bearsnake.k8sclient.K8SException;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.exceptions.ConfigurationException;
import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.exceptions.ProcessingException;
import com.liqid.k8s.plan.Plan;
import com.liqid.sdk.LiqidException;

public class ComposeCommand extends Command {

    public ComposeCommand(
        final Logger logger,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, force, timeoutInSeconds);
    }

    public ComposeCommand setProxyURL(final String value) {_proxyURL = value; return this; }

    @Override
    public Plan process(
    ) throws ConfigurationException,
             InternalErrorException,
             K8SException,
             LiqidException,
             ProcessingException {
        var fn = this.getClass().getName() + ":process";
        _logger.trace("Entering %s", fn);

        initK8sClient();
        initLiqidClient();
        getLiqidInventory();

        var errors = false;
        var plan = new Plan();

        if (errors && _force) {
            var ex = new ConfigurationException("Various configuration problems exist - processing will not continue.");
            _logger.throwing(ex);
            throw ex;
        }

        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }
}
