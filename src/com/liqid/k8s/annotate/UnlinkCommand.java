/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.annotate;

import com.bearsnake.k8sclient.K8SException;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.Command;
import com.liqid.k8s.exceptions.ProcessingException;
import com.liqid.k8s.plan.Plan;
import com.liqid.k8s.plan.actions.RemoveLinkage;

class UnlinkCommand extends Command {

    UnlinkCommand(
        final Logger logger,
        final String proxyURL,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, proxyURL, force, timeoutInSeconds);
    }

    @Override
    public Plan process() throws K8SException, ProcessingException {
        var fn = this.getClass().getName() + ":process";
        _logger.trace("Entering %s", fn);

        initK8sClient();

        // If there is no linkage, tell the user and stop
        if (!hasLinkage()) {
            System.err.println("WARNING:No linkage exists from this Kubernetes Cluster to the Liqid Cluster.");
            _logger.trace("Exiting %s with null", fn);
            return null;
        }

        var plan = new Plan().addAction(new RemoveLinkage());

        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }
}
