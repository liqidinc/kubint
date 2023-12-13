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
            var annotations = _k8sClient.getAnnotationsForNode(_nodeName);
        } catch (K8SHTTPError ex) {
            if (ex.getResponseCode() == 404) {
                System.err.println("ERROR:No Kubernetes worker node found with the name '" + _nodeName + "'");
                _logger.trace("Exiting %s false", fn);
                return false;
            }
        }

        // TODO

        _logger.trace("Exiting %s true", fn);
        return true;
    }
}
