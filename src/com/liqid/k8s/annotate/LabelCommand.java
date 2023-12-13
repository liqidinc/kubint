/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.annotate;

import com.bearsnake.klog.Logger;
import com.liqid.k8s.Command;

class LabelCommand extends Command {

    LabelCommand(
        final Logger logger,
        final String proxyURL,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, proxyURL, force, timeoutInSeconds);
    }

    @Override
    public void process() {
        var fn = "process";
        _logger.trace("Entering %s", fn);

        if (!initK8sClient()) {
            _logger.trace("Exiting %s", fn);
            return;
        }

        //  TODO

        _logger.trace("Exiting %s", fn);
    }
}
