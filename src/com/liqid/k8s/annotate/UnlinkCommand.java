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
import com.liqid.k8s.plan.Plan;

import static com.liqid.k8s.Constants.K8S_CONFIG_NAME;
import static com.liqid.k8s.Constants.K8S_CONFIG_NAMESPACE;
import static com.liqid.k8s.annotate.CommandType.UNLINK;

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
    public Plan process() throws K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = this.getClass().getName() + ":process";
        _logger.trace("Entering %s", fn);
        var plan = new Plan();

//        if (!initK8sClient()) {
//            _logger.trace("Exiting %s false", fn);
//            return false;
//        }
//
//        // If there is no configMap for this cluster-name, tell the user and stop
//        try {
//            _k8sClient.getConfigMap(K8S_CONFIG_NAMESPACE, K8S_CONFIG_NAME);
//        } catch (K8SHTTPError ex) {
//            if (ex.getResponseCode() == 404) {
//                System.err.println("ERROR:No linkage exists from this Kubernetes Cluster to the Liqid Cluster.");
//                _logger.trace("Exiting %s false", fn);
//                return false;
//            } else {
//                throw ex;
//            }
//        }
//
//        if (!checkForExistingAnnotations(UNLINK.getToken())) {
//            _logger.trace("Exiting %s false", fn);
//            return false;
//        }
//
//        clearLinkage();
//
        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }
}
