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
            // Note that we do NOT effect the removal of an annotation key by simply removing it from the container.
            // Instead, we have to keep the key, but set the value to null, and pass it all back for PATCH to work.
            // And YES, intelliJ, 'effect' *IS* the correct word here. Go away, you're drunk.
            var removed = removeAnnotationsFromNode(_nodeName);
            if (removed) {
                System.out.println("Removed Liqid annotations for worker '" + _nodeName + "'");
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
