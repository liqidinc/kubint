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

import static com.liqid.k8s.Constants.K8S_CONFIG_NAME;
import static com.liqid.k8s.Constants.K8S_CONFIG_NAMESPACE;
import static com.liqid.k8s.Constants.K8S_SECRET_NAME;
import static com.liqid.k8s.Constants.K8S_SECRET_NAMESPACE;
import static com.liqid.k8s.annotate.Application.UNLINK_COMMAND;

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
    public void process() throws K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = "process";
        _logger.trace("Entering %s", fn);

        if (!initK8sClient()) {
            _logger.trace("Exiting %s", fn);
            return;
        }

        // If there is no configMap for this cluster-name, tell the user and stop
        try {
            _k8sClient.getConfigMap(K8S_CONFIG_NAMESPACE, K8S_CONFIG_NAME);
        } catch (K8SHTTPError ex) {
            if (ex.getResponseCode() == 404) {
                System.err.println("ERROR:No linkage exists from this Kubernetes Cluster to the Liqid Cluster.");
                return;
            } else {
                throw ex;
            }
        }

        if (!checkForExistingAnnotations(UNLINK_COMMAND)) {
            _logger.trace("Exiting %s", fn);
            return;
        }

        // delete the configMap and secret
        _k8sClient.deleteConfigMap(K8S_CONFIG_NAMESPACE, K8S_CONFIG_NAME);
        try {
            _k8sClient.deleteSecret(K8S_SECRET_NAMESPACE, K8S_SECRET_NAME);
        } catch (K8SHTTPError ex) {
            //  If we got a 404, it's okay and even expected it there are no credentials.
            //  Non-404 is not okay.
            if (ex.getResponseCode() != 404) {
                throw ex;
            }
        }

        _logger.trace("Exiting %s", fn);
    }
}
