/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.commands;

import com.bearsnake.k8sclient.K8SException;
import com.bearsnake.k8sclient.K8SHTTPError;
import com.bearsnake.k8sclient.K8SJSONError;
import com.bearsnake.k8sclient.K8SRequestError;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.LiqidGeneralType;
import com.liqid.k8s.exceptions.ConfigurationDataException;
import com.liqid.k8s.exceptions.ConfigurationException;
import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.exceptions.ProcessingException;
import com.liqid.k8s.layout.ClusterLayout;
import com.liqid.k8s.layout.MachineProfile;
import com.liqid.k8s.plan.Plan;
import com.liqid.sdk.LiqidException;

import static com.liqid.k8s.Constants.K8S_ANNOTATION_MACHINE_NAME;

public class ComposeCommand extends Command {

    public ComposeCommand(
        final Logger logger,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, force, timeoutInSeconds);
    }

    public ComposeCommand setProxyURL(final String value) {_proxyURL = value; return this; }

    public ClusterLayout createDesiredLayout(
    ) throws K8SRequestError, K8SJSONError, K8SHTTPError {
        var fn = "createDesiredLayout";
        _logger.trace("Entering %s", fn);

        var errors = false;
        var errPrefix = getErrorPrefix();

        var layout = new ClusterLayout();
        var nodes = _k8sClient.getNodes();
        for (var node : nodes) {
            var machKey = createAnnotationKeyFor(K8S_ANNOTATION_MACHINE_NAME);
            var machName = node.metadata.annotations.get(machKey);
            if (machName != null) {
                var machine = _liqidInventory._machinesByName.get(machName);
                if (machine == null) {
                    System.out.printf("%s:Machine '%s' referenced by node '%s' does not exist in Liqid Cluster\n",
                                      errPrefix, machName, node.getName());
                    errors = true;
                }

                var machLayout = new MachineProfile(machine);
                for (var gType : LiqidGeneralType.values()) {
                    if (gType != LiqidGeneralType.CPU) {
                        var annoKey = createAnnotationKeyForDeviceType(gType);
                        var value = node.metadata.annotations.get(annoKey);
                        if (value != null) {
                            // look for 'acme:ft1000:2' or '3' in comma-separated list
                            var fmtError = false;
                            var entries = value.split(",");
                            for (var entry : entries) {
                                var split = entry.split(":");
                                if (split.length == 1) {
                                    //  just an integer (we hope)
                                    try {
                                        var count = Integer.parseInt(split[0]);
                                        machLayout.injectGenericCount(gType, count);
                                    } catch (NumberFormatException ex) {
                                        fmtError = true;
                                    }
                                } else if (split.length == 3) {
                                    //  vendor, model, and integer.
                                    try {
                                        var vendorName = split[0];
                                        var modelName = split[1];
                                        var count = Integer.parseInt(split[0]);
                                        machLayout.injectCount(gType, vendorName, modelName, count);
                                    } catch (NumberFormatException ex) {
                                        fmtError = true;
                                    }
                                } else {
                                    fmtError = true;
                                }
                            }

                            if (fmtError) {
                                System.out.printf("%s:Badly-formatted resource specification(s) in annotation for node '%s'",
                                                  errPrefix, node.getName());
                                errors = true;
                            }
                        }
                    }
                }

                layout.addMachineLayout(machLayout);
            }
        }

        if (errors && !_force) {
            layout = null;
        }

        _logger.trace("Exiting %s with %s", fn, layout);
        return layout;
    }

    private boolean checkDesiredLayout(
        final ClusterLayout existingClusterLayout,
        final ClusterLayout desiredClusterLayout
    ) {
        return true; //TODO
    }

    private Plan createPlan(
        final ClusterLayout existingClusterLayout,
        final ClusterLayout desiredClusterLayout
    ) {
        var fn = "createPlan";
        _logger.trace("Entering %s", fn);

        // TODO
        var plan = new Plan();

        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }

    @Override
    public Plan process(
    ) throws ConfigurationDataException,
             ConfigurationException,
             InternalErrorException,
             K8SException,
             LiqidException,
             ProcessingException {
        var fn = this.getClass().getName() + ":process";
        _logger.trace("Entering %s", fn);

        initK8sClient();

        // If there is no linkage, tell the user and stop
        if (!hasLinkage()) {
            throw new ConfigurationException("No linkage exists from this Kubernetes Cluster to the Liqid Cluster.");
        }

        getLiqidLinkage();
        initLiqidClient();
        getLiqidInventory();

        var groupId = _liqidInventory._groupsByName.get(_liqidGroupName).getGroupId();
        var currentLayout = ClusterLayout.createFromInventory(_liqidInventory, groupId);
        System.out.println("Current Layout:");
        currentLayout.show("| ");

        var desiredLayout = createDesiredLayout();
        if (desiredLayout == null) {
            throw new ConfigurationDataException("Various configuration problems exist - processing will not continue.");
        }
        System.out.println("Desired Layout:");
        desiredLayout.show("| ");

        var plan = createPlan(currentLayout, desiredLayout);

        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }
}
