/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.annotate;

import com.bearsnake.k8sclient.K8SHTTPError;
import com.bearsnake.k8sclient.K8SJSONError;
import com.bearsnake.k8sclient.K8SRequestError;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.Command;
import com.liqid.k8s.exceptions.ConfigurationDataException;
import com.liqid.k8s.exceptions.ConfigurationException;
import com.liqid.k8s.plan.Plan;
import com.liqid.sdk.LiqidException;

class ResourcesCommand extends Command {

    private boolean _allFlag = false;

    ResourcesCommand(
        final Logger logger,
        final String proxyURL,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, proxyURL, force, timeoutInSeconds);
    }

    ResourcesCommand setAll(final Boolean flag) { _allFlag = flag; return this; }

    @Override
    public Plan process(
    ) throws ConfigurationException,
             ConfigurationDataException,
             K8SHTTPError,
             K8SJSONError,
             K8SRequestError,
             LiqidException {
        var fn = this.getClass().getName() + ":process";
        _logger.trace("Entering %s", fn);
        var plan = new Plan();

//        if (!initK8sClient()) {
//            _logger.trace("Exiting %s false", fn);
//            return false;
//        }
//
//        if (!getLiqidLinkage()) {
//            throw new ConfigurationException("No linkage exists between the Kubernetes Cluster and a Liqid Cluster.");
//        }
//
//        if (!initLiqidClient()) {
//            System.err.println("ERROR:Cannot connect to the Liqid Cluster");
//            _logger.trace("Exiting %s false", fn);
//            return false;
//        }
//
//        _liqidInventory = getLiqidInventory(_liqidClient, _logger);
//        var groupParam = _allFlag ? null : _liqidInventory._groupsByName.get(_liqidGroupName);
//        displayDevices(groupParam);
//        displayMachines(groupParam);
//
//        // All done
//        logoutFromLiqidCluster();
        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }
}
