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
import com.liqid.sdk.LiqidException;

import static com.liqid.k8s.Constants.K8S_ANNOTATION_PREFIX;
import static com.liqid.k8s.Constants.K8S_CONFIG_NAME;
import static com.liqid.k8s.Constants.K8S_CONFIG_NAMESPACE;
import static com.liqid.k8s.annotate.CommandType.NODES;

class NodesCommand extends Command {

    NodesCommand(
        final Logger logger,
        final String proxyURL,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, proxyURL, force, timeoutInSeconds);
    }

    @Override
    public boolean process(
    ) throws ConfigurationException,
             ConfigurationDataException,
             K8SHTTPError,
             K8SJSONError,
             K8SRequestError,
             LiqidException {
        var fn = NODES.getToken() + ":process";
        _logger.trace("Entering %s", fn);

        if (!initK8sClient()) {
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        try {
            System.out.println("Liqid Cluster linkage settings:");
            var configMap = _k8sClient.getConfigMap(K8S_CONFIG_NAMESPACE, K8S_CONFIG_NAME);
            for (var entry : configMap.data.entrySet()) {
                System.out.printf("  %s: %s\n", entry.getKey(), entry.getValue());
            }
        } catch (K8SHTTPError ex) {
            if (ex.getResponseCode() == 404) {
                System.out.println("  <None established>");
            } else {
                throw ex;
            }
        }

        var nodes = _k8sClient.getNodes();
        for (var node : nodes) {
            var nodeName = node.metadata.name;
            System.out.println("Node " + nodeName + ":");
            var annotations = node.metadata.annotations;
            var hasEntry = false;
            for (var entry : annotations.entrySet()) {
                if (entry.getKey().startsWith(K8S_ANNOTATION_PREFIX)) {
                    System.out.println("  " + entry.getKey() + ": " + entry.getValue());
                    hasEntry = true;
                }
            }

            if (!hasEntry) {
                System.out.println("  <Node has no liqid-specific annotations>");
            }
        }

        _logger.trace("Exiting %s true", fn);
        return true;
    }
}
