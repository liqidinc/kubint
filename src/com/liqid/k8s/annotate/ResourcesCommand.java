/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.annotate;

import com.bearsnake.k8sclient.K8SException;
import com.bearsnake.k8sclient.K8SHTTPError;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.Command;
import com.liqid.k8s.exceptions.ConfigurationDataException;
import com.liqid.k8s.exceptions.ConfigurationException;
import com.liqid.k8s.exceptions.InternalErrorException;
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
    ) throws ConfigurationDataException,
             ConfigurationException,
             InternalErrorException,
             K8SException,
             LiqidException {
        var fn = this.getClass().getName() + ":process";
        _logger.trace("Entering %s", fn);

        initK8sClient();
        try {
            getLiqidLinkage();
        } catch (K8SHTTPError kex) {
            if (kex.getResponseCode() == 404) {
                throw new ConfigurationException("ERROR:No linkage exists between the Kubernetes Cluster and a Liqid Cluster");
            }
        }
        initLiqidClient();
        getLiqidInventory();

        var groupParam = _allFlag ? null : _liqidInventory._groupsByName.get(_liqidGroupName);
        System.out.println(groupParam);
        displayDevices(groupParam);
        displayMachines(groupParam);

        _logger.trace("Exiting %s with null", fn);
        return null;
    }
}
