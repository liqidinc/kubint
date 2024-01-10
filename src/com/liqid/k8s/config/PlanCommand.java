/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.config;

import com.bearsnake.k8sclient.K8SHTTPError;
import com.bearsnake.k8sclient.K8SJSONError;
import com.bearsnake.k8sclient.K8SRequestError;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.commands.Command;
import com.liqid.k8s.exceptions.ConfigurationDataException;
import com.liqid.k8s.exceptions.ConfigurationException;
import com.liqid.k8s.plan.Plan;
import com.liqid.sdk.LiqidException;

//class PlanCommand extends Command {
//
//    PlanCommand(
//        final Logger logger,
//        final String proxyURL,
//        final Integer timeoutInSeconds
//    ) {
//        super(logger, proxyURL, false, timeoutInSeconds);
//    }
//
//    @Override
//    public Plan process(
//    ) throws ConfigurationException,
//             ConfigurationDataException,
//             K8SHTTPError,
//             K8SJSONError,
//             K8SRequestError,
//             LiqidException {
//        var fn = this.getClass().getName() + ":process";
//        _logger.trace("Entering %s", fn);
//        var plan = new Plan();
//
////        if (!initK8sClient()) {
////            _logger.trace("Exiting %s false", fn);
////            return false;
////        }
////
////        if (!getLiqidLinkage()) {
////            _logger.trace("Exiting %s false", fn);
////            return false;
////        }
////
////        if (!initLiqidClient()) {
////            _logger.trace("Exiting %s false", fn);
////            return false;
////        }
////
////        _liqidInventory = getLiqidInventory(_liqidClient, _logger);
//
//        // TODO
//
//        _logger.trace("Exiting %s with %s", fn, plan);
//        return plan;
//    }
//}
