/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.config;

import com.bearsnake.k8sclient.K8SException;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.Command;
import com.liqid.k8s.exceptions.ConfigurationException;
import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.exceptions.ProcessingException;
import com.liqid.k8s.plan.Plan;
import com.liqid.k8s.plan.actions.ClearConfiguration;
import com.liqid.k8s.plan.actions.RemoveAllAnnotations;
import com.liqid.k8s.plan.actions.RemoveLinkage;
import com.liqid.sdk.LiqidException;

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

        if (!_force) {
            System.err.println("ERROR: -f,--force must be set to run this command");
            _logger.trace("Exiting %s with null", fn);
            return null;
        }

        var plan = new Plan();
        if (hasLinkage()) {
            plan.addAction(new RemoveLinkage());
        }
        if (hasAnnotations()) {
            plan.addAction(new RemoveAllAnnotations());
        }

        plan.addAction(new ClearConfiguration());

        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }
}
