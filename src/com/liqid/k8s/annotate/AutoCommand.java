/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.annotate;

import com.bearsnake.k8sclient.K8SHTTPError;
import com.bearsnake.k8sclient.K8SJSONError;
import com.bearsnake.k8sclient.K8SRequestError;
import com.bearsnake.k8sclient.Node;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.Command;
import com.liqid.k8s.GeneralType;
import com.liqid.k8s.exceptions.ConfigurationDataException;
import com.liqid.k8s.exceptions.ConfigurationException;
import com.liqid.sdk.DeviceStatus;
import com.liqid.sdk.DeviceType;
import com.liqid.sdk.Group;
import com.liqid.sdk.LiqidException;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.stream.Collectors;

import static com.liqid.k8s.Constants.K8S_ANNOTATION_MACHINE_NAME;
import static com.liqid.k8s.annotate.CommandType.AUTO;
import static com.liqid.k8s.plan.LiqidInventory.getLiqidInventory;

class AutoCommand extends Command {

    private Boolean _noUpdate = false;

    AutoCommand(
        final Logger logger,
        final String proxyURL,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, proxyURL, force, timeoutInSeconds);
    }

    AutoCommand setNoUpdate(final Boolean value) { _noUpdate = value; return this; }

    private boolean annotateAllocationsEqually(
        final Group group,
        final HashMap<Node, HashMap<String, String>> annotations
    ) {
        var fn = "annotateAllocationsEqually";
        _logger.trace("Entering %s with group=%s annotations=%s", fn, group, annotations);

        var devsByType = new HashMap<GeneralType, LinkedList<DeviceStatus>>();
        for (var ds : _liqidInventory._deviceStatusByGroupId.get(group.getGroupId())) {
            if (ds.getDeviceType() != DeviceType.COMPUTE) {
                var genType = GeneralType.fromDeviceType(ds.getDeviceType());
                if (!devsByType.containsKey(genType)) {
                    devsByType.put(genType, new LinkedList<>());
                }
                devsByType.get(genType).add(ds);
            }
        }

        System.out.printf("Partitioning Resources among %d worker nodes...\n", annotations.size());
        for (var dbtEntry : devsByType.entrySet()) {
            var genType = dbtEntry.getKey();
            var devs = dbtEntry.getValue();
            var devCount = devs.size();
            var workerCount = annotations.size();
            System.out.printf("  %s count = %d\n", dbtEntry.getKey(), devCount);

            for (var annos : annotations.values()) {
                var resCount = devCount / workerCount;
                if (devCount % workerCount > 0) {
                    resCount++;
                }

                var annoKey = createAnnotationKeyForDeviceType(genType);
                var annoValue = String.format("%d", resCount);
                annos.put(annoKey, annoValue);

                workerCount--;
                devCount -= resCount;
                if ((workerCount == 0) || (devCount == 0)) {
                    break;
                }
            }
        }

        _logger.trace("Exiting %s true", fn);
        return true;
    }

    private boolean precheck(
    ) throws ConfigurationDataException,
             ConfigurationException,
             K8SHTTPError,
             K8SJSONError,
             K8SRequestError,
             LiqidException {
        var fn = "precheck";
        _logger.trace("Entering %s", fn);

        if (!initK8sClient()) {
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        if (!getLiqidLinkage()) {
            throw new ConfigurationException("No linkage exists between the Kubernetes Cluster and a Liqid Cluster.");
        }

        if (!initLiqidClient()) {
            System.err.println("ERROR:Cannot connect to the Liqid Cluster");
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        // Go grab the Liqid config first, so that we can stop here if something is wrong with Liqid.
        // Then make sure there aren't any existing annotations in the way.
        _liqidInventory = getLiqidInventory(_liqidClient, _logger);

        if (!checkForExistingAnnotations(AUTO.getToken())) {
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        _logger.trace("Exiting %s true", fn);
        return true;
    }

    @Override
    public boolean process(
    ) throws ConfigurationException,
             ConfigurationDataException,
             K8SHTTPError,
             K8SJSONError,
             K8SRequestError,
             LiqidException {
        var fn = AUTO.getToken() + ":process";
        _logger.trace("Entering %s", fn);

        if (!precheck()) {
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        // We'll need the Liqid Cluster group, and we're going to start building up annotations
        // as maps of keys to values, one map per worker node.
        var group = _liqidInventory._groupsByName.get(_liqidGroupName);

        // Get the worker nodes from Kubernetes, then create a map of them keyed by node name.
        var workerNodes = _k8sClient.getNodes();
        if (workerNodes.isEmpty()) {
            System.err.println("ERROR:Kubernetes cluster is not reporting any worker nodes");
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        var workerNodesByName =
            workerNodes.stream().collect(Collectors.toMap(Node::getName, wn -> wn, (a, b) -> b, HashMap::new));

        // Go find the CPU resources, check the descriptions, and create a map of the ones which refer to
        // actual worker nodes (as discovered above).
        var errPrefix = _force ? "WARNING" : "ERROR";
        var errors = false;
        var deviceStatuses = _liqidInventory._deviceStatusByGroupId.get(group.getGroupId());
        var cpuToNode = new HashMap<DeviceStatus, Node>();
        for (var ds : deviceStatuses) {
            var di = _liqidInventory._deviceInfoById.get(ds.getDeviceId());
            if (ds.getDeviceType() == DeviceType.COMPUTE) {
                var workerName = di.getUserDescription();
                if (workerName.isEmpty() || workerName.equals("n/a")) {
                    System.err.printf("%s:CPU Resource '%s' has no description\n", errPrefix, ds.getName());
                    if (!_force) {
                        errors = true;
                    }
                } else if (!workerNodesByName.containsKey(workerName)) {
                    System.err.printf("ERROR:CPU Resource '%s' refers to '%s' which is not a discovered worker node\n",
                                      ds.getName(), workerName);
                    if (!_force) {
                        errors = true;
                    }
                } else {
                    cpuToNode.put(ds, workerNodesByName.get(workerName));
                }
            }
        }

        // Start building up annotations.
        // The outer map has an entry per identified worker node, keyed by node name.
        // The content comprises the annotations for that node, keyed by annotation key.
        var annotations = new HashMap<Node, HashMap<String, String>>();
        for (var entry : cpuToNode.entrySet()) {
            var cpu = entry.getKey();
            var node = entry.getValue();

            var machineId = _liqidInventory._deviceRelationsByDeviceId.get(cpu.getDeviceId())._machineId;
            if (machineId == null) {
                System.err.printf("ERROR:CPU Resource '%s' is not attached to a machine\n", cpu.getName());
                if (!_force) {
                    errors = true;
                }
            } else {
                var machName = _liqidInventory._machinesById.get(machineId).getMachineName();
                var newMap = new HashMap<String, String>();
                newMap.put(createAnnotationKeyFor(K8S_ANNOTATION_MACHINE_NAME), machName);
                annotations.put(node, newMap);
            }
        }

        if (errors) {
            System.err.println("Errors prevent further processing.");
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        // Decide how to allocate resources
        if (!annotateAllocationsEqually(group, annotations)) {
            System.err.println("Errors prevent further processing.");
            _logger.trace("Exiting %s false", fn);
            return false;
        }

        // Show the user what we're going to do
        var verb = _noUpdate ? "would" : "will";
        System.out.println();
        System.out.printf("The following annotations %s be written:\n", verb);
        for (var entry : annotations.entrySet()) {
            var node = entry.getKey();
            System.out.printf("  For Node %s:\n", node.getName());
            for (var anno : entry.getValue().entrySet()) {
                System.out.printf("    %s=%s\n", anno.getKey(), anno.getValue());
            }
        }

        // Persist the annotations to Kubernetes
        if (!_noUpdate) {
            System.out.println();
            System.out.println("Writing annotations...");
            for (var entry : annotations.entrySet()) {
                var node = entry.getKey();
                var annos = entry.getValue();
                _k8sClient.updateAnnotationsForNode(node.getName(), annos);
            }
        }

        // All done
        logoutFromLiqidCluster();
        _logger.trace("Exiting %s true", fn);
        return true;
    }
}
