/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.commands;

import com.bearsnake.k8sclient.K8SException;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.exceptions.ProcessingException;
import com.liqid.k8s.plan.Plan;
import com.liqid.k8s.plan.actions.CreateGroupAction;
import com.liqid.k8s.plan.actions.CreateLinkageAction;
import com.liqid.k8s.plan.actions.RemoveAllAnnotationsAction;
import com.liqid.k8s.plan.actions.RemoveLinkageAction;
import com.liqid.sdk.LiqidException;

public class LinkCommand extends Command {

    public LinkCommand(
        final Logger logger,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, force, timeoutInSeconds);
    }

    public LinkCommand setLiqidAddress(final String value) { _liqidAddress = value; return this; }
    public LinkCommand setLiqidGroupName(final String value) { _liqidGroupName = value; return this; }
    public LinkCommand setLiqidPassword(final String value) { _liqidPassword = value; return this; }
    public LinkCommand setLiqidUsername(final String value) { _liqidUsername = value; return this; }
    public LinkCommand setProxyURL(final String value) { _proxyURL = value; return this; }

    private boolean preCheck(
        final Plan plan
    ) throws InternalErrorException, K8SException {
        var fn = "preCheck";
        _logger.trace("Entering %s", fn);

        var errPrefix = getErrorPrefix();
        var errors = false;

        // check for existing linkage
        if (hasLinkage()) {
            var msg = "Linkage already exists between the Kubernetes Cluster and the Liqid Cluster";
            System.err.printf("%s:%s\n", errPrefix, msg);
            if (!_force) {
                errors = true;
            }

            plan.addAction(new RemoveLinkageAction());
        }

        // check for existing annotations
        var hasAnnotations = hasAnnotations(_k8sClient.getNodes());
        if (hasAnnotations) {
            var msg = "Liqid annotations already exist on nodes in the Kubernetes Cluster";
            System.err.printf("%s:%s\n", errPrefix, msg);
            if (!_force) {
                errors = true;
            }

            plan.addAction(new RemoveAllAnnotationsAction());
        }


        // can we talk to the Liqid cluster? does the indicated group exist?
        try {
            initLiqidClient();
            var groups = _liqidClient.getGroups();
            var groupExists = groups.stream().anyMatch(group -> group.getGroupName().equals(_liqidGroupName));
            if (!groupExists) {
                var msg = "Group " + _liqidGroupName + " does not exist on the Liqid Cluster";
                System.err.printf("%s:%s\n", errPrefix, msg);
                if (!_force) {
                    errors = true;
                }

                plan.addAction(new CreateGroupAction().setGroupName(_liqidGroupName));
            }
        } catch (LiqidException lex) {
            var msg = "Cannot communicate properly with the Liqid Cluster";
            System.err.printf("%s:%s\n", errPrefix, msg);
            if (!_force) {
                errors = true;
            }
        }

        var result = !errors;
        _logger.trace("Exiting %s with %s", fn, result);
        return result;
    }

    @Override
    public Plan process() throws InternalErrorException, K8SException, LiqidException, ProcessingException {
        var fn = this.getClass().getName() + ":process";
        _logger.trace("Entering %s", fn);

        initK8sClient();

        var plan = new Plan();
        if (!preCheck(plan)) {
            var ex = new ProcessingException();
            _logger.throwing(ex);
            throw ex;
        }

        plan.addAction(new CreateLinkageAction().setLiqidAddress(_liqidAddress)
                                                .setLiqidGroupName(_liqidGroupName)
                                                .setLiqidUsername(_liqidUsername)
                                                .setLiqidPassword(_liqidPassword));

        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }
}
