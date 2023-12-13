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

import static com.liqid.k8s.annotate.Application.AUTO_COMMAND;

class AutoCommand extends Command {

    AutoCommand(
        final Logger logger,
        final String proxyURL,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, proxyURL, force, timeoutInSeconds);
    }

    @Override
    public void process(
    ) throws ConfigurationException,
             ConfigurationDataException,
             K8SHTTPError,
             K8SJSONError,
             K8SRequestError {
        var fn = "process";
        _logger.trace("Entering %s", fn);

        if (!initK8sClient()) {
            _logger.trace("Exiting %s", fn);
            return;
        }

        if (!getLiqidLinkage()) {
            throw new ConfigurationException("No linkage exists between the Kubernetes Cluster and a Liqid Cluster.");
        }

        if (!initLiqidClient()) {
            System.err.println("ERROR:Cannot connect to the Liqid Cluster");
            _logger.trace("Exiting %s", fn);
            return;
        }

        if (!checkForExistingAnnotations(AUTO_COMMAND)) {
            _logger.trace("Exiting %s", fn);
            return;
        }

        //  TODO

        // All done
        logoutFromLiqidCluster();
        _logger.trace("Exiting %s", fn);
    }
}
