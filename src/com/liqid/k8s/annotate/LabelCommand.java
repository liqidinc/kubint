/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.annotate;

import com.bearsnake.klog.Logger;
import com.liqid.k8s.Command;

import static com.liqid.k8s.annotate.CommandType.LABEL;

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
    public boolean process() {
        var fn = LABEL.getToken() + ":process";
        _logger.trace("Entering %s", fn);

        if (!initK8sClient()) {
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        //  TODO

        _logger.trace("Exiting %s true", fn);
        return true;
    }
}
