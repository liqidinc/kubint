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
import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.plan.Plan;
import com.liqid.sdk.LiqidException;

import static com.liqid.k8s.annotate.CommandType.LINK;

class LinkCommand extends Command {

    LinkCommand(
        final Logger logger,
        final String proxyURL,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, proxyURL, force, timeoutInSeconds);
    }

    LinkCommand setLiqidAddress(final String value) { _liqidAddress = value; return this; }
    LinkCommand setLiqidGroupName(final String value) { _liqidGroupName = value; return this; }
    LinkCommand setLiqidPassword(final String value) { _liqidPassword = value; return this; }
    LinkCommand setLiqidUsername(final String value) { _liqidUsername = value; return this; }

    @Override
    public Plan process() throws InternalErrorException, K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = this.getClass().getName() + ":process";
        _logger.trace("Entering %s", fn);
        var plan = new Plan();

//        if (_liqidAddress == null) {
//            throw new InternalErrorException("Liqid Address is null");
//        }
//
//        if (_liqidGroupName == null) {
//            throw new InternalErrorException("Liqid Group name is null");
//        }
//
//        if (!initK8sClient()) {
//            _logger.trace("Exiting %s false", fn);
//            return false;
//        }
//
//        // There should not be any current linkages nor annotations
//        if (!checkForExistingLinkage(LINK.getToken())) {
//            _logger.trace("Exiting %s false", fn);
//            return false;
//        }
//
//        if (!checkForExistingAnnotations(LINK.getToken())) {
//            _logger.trace("Exiting %s false", fn);
//            return false;
//        }
//
//        // Now go verify that the link is correct (i.e., that we can contact the Liqid Director)
//        try {
//            if (!initLiqidClient()) {
//                if (_force) {
//                    System.err.println("WARNING:Cannot connect to the Liqid Cluster, but proceeding anyway.");
//                } else {
//                    System.err.println("ERROR:Cannot connect to the Liqid Cluster - stopping.");
//                    _logger.trace("Exiting %s false", fn);
//                    return false;
//                }
//            } else {
//                // Logged in - see if the indicated group exists.
//                var groups = _liqidClient.getGroups();
//                boolean found = false;
//                for (var group : groups) {
//                    if (group.getGroupName().equals(_liqidGroupName)) {
//                        found = true;
//                        break;
//                    }
//                }
//
//                if (!found) {
//                    if (_force) {
//                        System.err.println("WARNING:Group " + _liqidGroupName + " does not exist on the Liqid Cluster and will be created");
//                        _liqidClient.createGroup(_liqidGroupName);
//                    } else {
//                        System.err.println("ERROR:Group " + _liqidGroupName + " does not exist on the Liqid Cluster");
//                        _logger.trace("Exiting %s false", fn);
//                        return false;
//                    }
//                }
//            }
//        } catch (LiqidException ex) {
//            _logger.catching(ex);
//            if (_force) {
//                System.err.println("WARNING:Cannot connect to Liqid Cluster - proceeding anyway due to force being set");
//            } else {
//                System.err.println("ERROR:Cannot connect to Liqid Cluster - stopping");
//                _logger.trace("Exiting %s false", fn);
//                return false;
//            }
//        }
//
//        createLinkage();
//
//        // All done
//        logoutFromLiqidCluster();
        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }
}
