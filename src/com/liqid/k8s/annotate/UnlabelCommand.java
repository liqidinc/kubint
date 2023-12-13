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

import static com.liqid.k8s.Constants.K8S_ANNOTATION_KEYS;
import static com.liqid.k8s.Constants.K8S_ANNOTATION_PREFIX;
import static com.liqid.k8s.annotate.CommandType.UNLABEL;

class UnlabelCommand extends Command {

    private String _nodeName;

    UnlabelCommand(
        final Logger logger,
        final String proxyURL,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, proxyURL, force, timeoutInSeconds);
    }

    UnlabelCommand setNodeName(final String value) { _nodeName = value; return this; }

    @Override
    public boolean process() throws K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = UNLABEL.getToken() + ":process";
        _logger.trace("Entering %s", fn);

        if (!initK8sClient()) {
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        try {
            // Note that we do NOT effect removal of an annotation key by simply removing it from the container.
            // Instead, we have to keep the key, but set the value to null, and pass it all back for PATCH to work.
            var annotations = _k8sClient.getAnnotationsForNode(_nodeName);
            var changed = false;
            for (java.util.Map.Entry<String, String> entry : annotations.entrySet()) {
                if (entry.getKey().startsWith(K8S_ANNOTATION_PREFIX)) {
                    annotations.put(entry.getKey(), null);
                    changed = true;
                }
            }
            if (changed) {
                System.out.println("Removing annotations for worker '" + _nodeName + "'...");
                _k8sClient.updateAnnotationsForNode(_nodeName, annotations);
            } else {
                System.out.println("No Liqid annotations exist for worker '" + _nodeName + "'");
            }
        } catch (K8SHTTPError ex) {
            if (ex.getResponseCode() == 404) {
                System.err.println("ERROR:No Kubernetes worker node found with the name '" + _nodeName + "'");
                _logger.trace("Exiting %s false", fn);
                return false;
            }
        }

        _logger.trace("Exiting %s true", fn);
        return true;
    }
}
