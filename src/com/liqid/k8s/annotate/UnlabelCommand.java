/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.annotate;

import com.bearsnake.k8sclient.K8SException;
import com.bearsnake.k8sclient.K8SHTTPError;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.Command;
import com.liqid.k8s.plan.Plan;
import com.liqid.k8s.plan.actions.RemoveAllAnnotations;
import com.liqid.k8s.plan.actions.RemoveAnnotations;

class UnlabelCommand extends Command {

    private Boolean _allFlag = false;
    private String _nodeName = null;

    UnlabelCommand(
        final Logger logger,
        final String proxyURL,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, proxyURL, force, timeoutInSeconds);
    }

    UnlabelCommand setAll(final Boolean flag) { _allFlag = flag; return this; }
    UnlabelCommand setNodeName(final String value) { _nodeName = value; return this; }

    @Override
    public Plan process() throws K8SException {
        var fn = this.getClass().getName() + ":process";
        _logger.trace("Entering %s", fn);

        if ((_nodeName != null) && _allFlag) {
            System.err.println("WARNING:All flag ignored with node specification.");
            _allFlag = false;
        }

        initK8sClient();
        if (!hasAnnotations()) {
            System.err.println("WARNING:No linkage exists from this Kubernetes Cluster to the Liqid Cluster.");
        }

        if (_nodeName != null) {
            try {
                _k8sClient.getNode(_nodeName);
            } catch (K8SHTTPError kex) {
                if (kex.getResponseCode() == 404) {
                    System.err.printf("ERROR:Node %s does not exist in the Kubernetes Cluster\n", _nodeName);
                    _logger.trace("Exiting %s with null", fn);
                    return null;
                } else {
                    throw kex;
                }
            }
        }

        var plan = new Plan();
        if (_allFlag) {
            plan.addAction(new RemoveAllAnnotations());
        } else {
            plan.addAction(new RemoveAnnotations().addNodeName(_nodeName));
        }

        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }
}
