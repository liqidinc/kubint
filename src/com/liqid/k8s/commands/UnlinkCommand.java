/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.commands;

import com.bearsnake.k8sclient.K8SException;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.exceptions.ConfigurationDataException;
import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.exceptions.ProcessingException;
import com.liqid.k8s.plan.Plan;
import com.liqid.k8s.plan.actions.RemoveLinkage;
import com.liqid.sdk.LiqidException;

public class UnlinkCommand extends Command {

    public UnlinkCommand(
        final Logger logger,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, force, timeoutInSeconds);
    }

    public UnlinkCommand setProxyURL(final String value) { _proxyURL = value; return this; }

    @Override
    public Plan process(
    ) throws ConfigurationDataException,
             InternalErrorException,
             K8SException,
             LiqidException,
             ProcessingException {
        var fn = this.getClass().getName() + ":process";
        _logger.trace("Entering %s", fn);

//        initK8sClient();
//
//        // If there is no linkage, tell the user and stop
//        if (!hasLinkage()) {
//            System.err.println("WARNING:No linkage exists from this Kubernetes Cluster to the Liqid Cluster.");
//            _logger.trace("Exiting %s with null", fn);
//            return null;
//        }
//
//        getLiqidLinkage();
//        initLiqidClient();
//        var plan = new Plan().addAction(new RemoveLinkage());
        var plan = new Plan();

        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }
}
