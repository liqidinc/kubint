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
import com.liqid.k8s.plan.actions.ClearConfigurationAction;
import com.liqid.k8s.plan.actions.RemoveAllAnnotationsAction;
import com.liqid.k8s.plan.actions.RemoveLinkageAction;
import com.liqid.sdk.LiqidException;

public class ResetCommand extends Command {

    public ResetCommand(
        final Logger logger,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, force, timeoutInSeconds);
    }

    public ResetCommand setLiqidAddress(final String value) { _liqidAddress = value; return this; }
    public ResetCommand setLiqidPassword(final String value) { _liqidPassword = value; return this; }
    public ResetCommand setLiqidUsername(final String value) { _liqidUsername = value; return this; }
    public ResetCommand setProxyURL(final String value) { _proxyURL = value; return this; }

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

        if (!_force) {
            System.err.println("ERROR: -f,--force must be set to run this command");
            _logger.trace("Exiting %s with null", fn);
            return null;
        }

        var plan = new Plan();
        if (hasLinkage()) {
            plan.addAction(new RemoveLinkageAction());
        }
        if (hasAnnotations()) {
            plan.addAction(new RemoveAllAnnotationsAction());
        }

        plan.addAction(new ClearConfigurationAction());

        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }
}
