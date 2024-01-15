/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.commands;

import com.bearsnake.k8sclient.K8SException;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.exceptions.ConfigurationDataException;
import com.liqid.k8s.exceptions.ConfigurationException;
import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.exceptions.ProcessingException;
import com.liqid.k8s.plan.Plan;
import com.liqid.sdk.LiqidException;

public class ComposeCommand extends Command {

    public ComposeCommand(
        final Logger logger,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, force, timeoutInSeconds);
    }

    public ComposeCommand setProxyURL(final String value) {_proxyURL = value; return this; }

    //TODO
//    private boolean checkDesiredLayout(
//        final ClusterLayout existingClusterLayout,
//        final ClusterLayout desiredClusterLayout
//    ) {
//        var fn = "checkDesiredLayout";
//        _logger.trace("Entering %s", fn);
//
//        var errors = false;
//        var errPrefix = getErrorPrefix();
//
//        var existingFlat = existingClusterLayout.getFlattenedProfile();
//        var desiredFlat = desiredClusterLayout.getFlattenedProfile();
//
//        System.out.println("Existing Inventory:");
//        existingFlat.show("| ");
//
//        System.out.println("Total Desired Inventory:");
//        desiredFlat.show("| ");
//
//        var totalsByType = Arrays.stream(LiqidGeneralType.values())
//                                 .collect(Collectors.toMap(type -> type, type -> 0, (a, b) -> b, HashMap::new));
//        for (var resModel : existingFlat.getResourceModels()) {
//            var genType = resModel.getGeneralType();
//            var count = totalsByType.get(genType) + existingFlat.getCount(resModel);
//            totalsByType.put(genType, count);
//        }
//
//        for (var resModel : desiredFlat.getResourceModels()) {
//            if (!resModel.isGeneric()) {
//                var desiredCount = desiredFlat.getCount(resModel);
//                var existCount = existingFlat.getCount(resModel);
//                var used = desiredCount;
//                if (desiredCount > existCount) {
//                    System.err.printf("%s:Asking for %d of %s, but we have only %d\n",
//                                      errPrefix, desiredCount, resModel.toString(), existCount);
//                    errors = true;
//                    used = existCount;
//                }
//
//                var newCount = totalsByType.get(resModel.getGeneralType()) - used;
//                totalsByType.put(resModel.getGeneralType(), newCount);
//            }
//        }
//
//        for (var resModel : desiredFlat.getResourceModels()) {
//            if (resModel.isGeneric()) {
//                var desiredCount = desiredFlat.getCount(resModel);
//                var genType = resModel.getGeneralType();
//                var existCount = totalsByType.get(genType);
//                if (desiredCount > existCount) {
//                    System.err.printf("%s:Asking for %d of generic %s, but we have only %d\n",
//                                      errPrefix, desiredCount, genType, existCount);
//                    errors = true;
//                }
//            }
//        }
//
//        var result = !errors;
//        _logger.trace("Exiting %s with %s", fn, result);
//        return result;
//    }
//
//    public ClusterLayout createDesiredLayout(
//    ) throws K8SRequestError, K8SJSONError, K8SHTTPError {
//        var fn = "createDesiredLayout";
//        _logger.trace("Entering %s", fn);
//
//        var errors = false;
//        var errPrefix = getErrorPrefix();
//
//        var layout = new ClusterLayout();
//        var nodes = _k8sClient.getNodes();
//        for (var node : nodes) {
//            var machKey = createAnnotationKeyFor(K8S_ANNOTATION_MACHINE_NAME);
//            var machName = node.metadata.annotations.get(machKey);
//            if (machName != null) {
//                var machine = _liqidInventory._machinesByName.get(machName);
//                if (machine == null) {
//                    System.err.printf("%s:Machine '%s' referenced by node '%s' does not exist in Liqid Cluster\n",
//                                      errPrefix, machName, node.getName());
//                    errors = true;
//                }
//
//                var machLayout = new MachineProfile(machine);
//                for (var gType : LiqidGeneralType.values()) {
//                    if (gType != LiqidGeneralType.CPU) {
//                        var annoKey = createAnnotationKeyForDeviceType(gType);
//                        var value = node.metadata.annotations.get(annoKey);
//                        if (value != null) {
//                            // look for 'acme:ft1000:2' or '3' in comma-separated list
//                            var fmtError = false;
//                            var entries = value.split(",");
//                            for (var entry : entries) {
//                                var split = entry.split(":");
//                                if (split.length == 1) {
//                                    //  just an integer (we hope)
//                                    try {
//                                        var count = Integer.parseInt(split[0]);
//                                        machLayout.injectCount(gType, count);
//                                    } catch (NumberFormatException ex) {
//                                        fmtError = true;
//                                    }
//                                } else if (split.length == 3) {
//                                    //  vendor, model, and integer.
//                                    try {
//                                        var vendorName = split[0];
//                                        var modelName = split[1];
//                                        var count = Integer.parseInt(split[0]);
//                                        machLayout.injectCount(gType, vendorName, modelName, count);
//                                    } catch (NumberFormatException ex) {
//                                        fmtError = true;
//                                    }
//                                } else {
//                                    fmtError = true;
//                                }
//                            }
//
//                            if (fmtError) {
//                                System.err.printf("%s:Badly-formatted resource specification(s) in annotation for node '%s'",
//                                                  errPrefix, node.getName());
//                                errors = true;
//                            }
//                        }
//                    }
//                }
//
//                layout.addMachineProfile(machLayout);
//            }
//        }
//
//        if (errors && !_force) {
//            layout = null;
//        }
//
//        _logger.trace("Exiting %s with %s", fn, layout);
//        return layout;
//    }
//
//    /**
//     * Create an ordered set of action steps which will result in effecting the given variances
//     */
//    private Plan createPlan(
//        final ClusterLayout desiredLayout
//    ) throws InternalErrorException {
//        var fn = "createPlan";
//        _logger.trace("Entering %s", fn);
//
//        var assignedResources = new HashMap<Machine, LinkedList<DeviceInfo>>();
//        var unassignedResources = new LinkedList<DeviceInfo>();
//        determineDeviceAllocation(desiredLayout, unassignedResources, assignedResources);
//        var variances = VarianceSet.createVarianceSet(_liqidInventory, assignedResources);
//
//        var plan = new Plan();
//        var availableDevices = unassignedResources.stream()
//                                                  .map(DeviceInfo::getDeviceIdentifier)
//                                                  .collect(Collectors.toCollection(HashSet::new));
//        while (!variances.isEmpty()) {
//            plan.addAction(variances.getAction(_liqidInventory, availableDevices));
//        }
//
//        _logger.trace("Exiting %s with %s", fn, plan);
//        return plan;
//    }
//
//    //TODO temporary func
//    private void show(
//        final String caption,
//        final List<DeviceInfo> unasg,
//        final Map<Machine, LinkedList<DeviceInfo>> asg
//    ) {
//        System.out.printf("------------------- %s -------------------\n", caption);
//        System.out.printf("Unassigned: %s\n",
//                          unasg.stream().map(DeviceInfo::getName).collect(Collectors.toCollection(LinkedList::new)));
//        System.out.println("Assigned:");
//        for (var e : asg.entrySet()) {
//            System.out.printf("  %s: %s\n",
//                              e.getKey().getMachineName(),
//                              e.getValue().stream().map(DeviceInfo::getName).collect(Collectors.toCollection(LinkedList::new)));
//        }
//    }
//
//    /**
//     * Based on the given desired cluster layout (which refers to types, vendors and models ONLY),
//     * and the current inventory in _liqidInventory, we populate unassignedResources and assignedResources
//     * to indicate which devices are supposed to end up where.
//     * @param desiredClusterLayout ClusterLayout indicated the desired layout
//     * @param unassignedResources a list representing the devices which are to become or remain unassigned
//     * @param assignedResources a map of machines -> list of devices which are to become or remain assigned to that machine
//     */
//    private void determineDeviceAllocation(
//        final ClusterLayout desiredClusterLayout,
//        final List<DeviceInfo> unassignedResources,
//        final Map<Machine, LinkedList<DeviceInfo>> assignedResources
//    ) {
//        var fn = "determineDeviceAllocation";
//        _logger.trace("Entering %s with %s", fn, desiredClusterLayout);
//
//        // Populate unassignedResources and assignedResources according to _liqidInventory.
//        // This is our starting point.
//        var groupId = _liqidInventory._groupsByName.get(_liqidGroupName).getGroupId();
//        for (var machine : _liqidInventory._machinesById.values()) {
//            if (Objects.equals(machine.getGroupId(), groupId)) {
//                var devInfos = new LinkedList<DeviceInfo>();
//                for (var ds : _liqidInventory._deviceStatusByMachineId.get(machine.getMachineId())) {
//                    if (ds.getDeviceType() != DeviceType.COMPUTE) {
//                        devInfos.add(_liqidInventory._deviceInfoById.get(ds.getDeviceId()));
//                    }
//                }
//                assignedResources.put(machine, devInfos);
//            }
//        }
//
//        for (var devRel : _liqidInventory._deviceRelationsByDeviceId.values()) {
//            if (devRel._groupId.equals(groupId)) {
//                if (devRel._machineId == null) {
//                    unassignedResources.add(_liqidInventory._deviceInfoById.get(devRel._deviceId));
//                }
//            }
//        }
//
//        show("Starting point", unassignedResources, assignedResources);//TODO remove
//
//
//        // TODO the following isn't quite right - it can take generics from a machine for some other machine's
//        //  specifics (or generics, actually), given the first machine something else, which might end up being
//        //  the same vendor/model, just a different device. That is not what we want to do.
////        unassignedResources.addAll(_liqidInventory._deviceInfoById.values());
////        show("To Begin With", unassignedResources, assignedResources);//TODO remove
////
////        // Per machine, Keep what we already have, that we've specifically asked for.
////        // This helps prevent unnecessary allocation of devices.
////        // No generics - we do that later.
////        for (var mach : _liqidInventory._machinesById.values()) {
////            var desiredProfile = desiredClusterLayout.getMachineProfile(mach.getMachineId());
////            for (var resModel : desiredProfile.getResourceModels()) {
////                if (!resModel.isGeneric()) {
////                    var count = desiredProfile.getCount(resModel);
////                    while ((count > 0) && (moveUnassignedDeviceInfoToMachine(assignedResources,
////                                                                             unassignedResources,
////                                                                             resModel,
////                                                                             mach,
////                                                                             true))) {
////                        count--;
////                    }
////                }
////            }
////        }
////
////        show("After preserving specifics", unassignedResources, assignedResources);//TODO remove
////
////        // Now per machine, allocate the specifics which we do not yet have.
////        // We wait until now to do this, so that we don't allocate something to machine X which was already
////        // on machine Y, and is staying on machine Y.
////        for (var mach : _liqidInventory._machinesById.values()) {
////            var desiredProfile = desiredClusterLayout.getMachineProfile(mach.getMachineId());
////            for (var resModel : desiredProfile.getResourceModels()) {
////                if (!resModel.isGeneric()) {
////                    var count = desiredProfile.getCount(resModel);
////                    while ((count > 0) && moveUnassignedDeviceInfoToMachine(assignedResources,
////                                                                            unassignedResources,
////                                                                            resModel,
////                                                                            mach,
////                                                                            false)) {
////                        count--;
////                    }
////                }
////            }
////        }
////
////        show("After assigning remaining specifics", unassignedResources, assignedResources);//TODO remove
////
////        // Now (once again per machine) we allocate generics.
////        for (var mach : _liqidInventory._machinesById.values()) {
////            var desiredProfile = desiredClusterLayout.getMachineProfile(mach.getMachineId());
////            for (var resModel : desiredProfile.getResourceModels()) {
////                if (resModel.isGeneric()) {
////                    var count = desiredProfile.getCount(resModel);
////                    while ((count > 0) && moveUnassignedDeviceInfoToMachine(assignedResources,
////                                                                            unassignedResources,
////                                                                            resModel.getGeneralType(),
////                                                                            mach)) {
////                        count--;
////                    }
////                }
////            }
////        }
////
////        show("After assigning generics", unassignedResources, assignedResources);//TODO remove
//
//        //  At this point, _assignedDeviceInfos is an accurate representation of which devices
//        //  should be assigned to which machines.
//
//        _logger.trace("Exiting %s", fn);
//    }
//
//    /**
//     * Moves a DeviceInfo object of the requested ResModel, from the unassigned list
//     * to the assigned list for the given machine.
//     * This refers to _unassignedResources and _assignedResources containers.
//     * @param assignedResources map of resources which are assigned to machines
//     * @param unassignedResources collection of resources which are *not* assigned to machines
//     * @param resModel Indicates the model we want - cannot be a generic model
//     * @param machine machine to which the device info is assigned
//     * @param requireMachine indicates that we will *only* look for deviceInfo objects which are already in the
//     *                       indicated machine according to _liqidInventory.
//     * @return true if we made the re-assignment, else false
//     */
//    private boolean moveUnassignedDeviceInfoToMachine(
//        final Map<Machine, LinkedList<DeviceInfo>> assignedResources,
//        final List<DeviceInfo> unassignedResources,
//        final ResourceModel resModel,
//        final Machine machine,
//        final boolean requireMachine
//    ) {
//        var fn = "moveUnassignedDeviceInfoToMachine";
//        _logger.trace("Entering %s with resModel=%s machine=%s requireMachine=%s",
//                      fn, resModel, machine, requireMachine);
//
//        for (var di : unassignedResources) {
//            if (new ResourceModel(di).equals(resModel)) {
//                var ok = true;
//                if (requireMachine) {
//                    var relation = _liqidInventory._deviceRelationsByDeviceId.get(di.getDeviceIdentifier());
//                    ok = Objects.equals(relation._machineId, machine.getMachineId());
//                }
//
//                if (ok) {
//                    _logger.trace("Assigning %s device %s to %s",
//                                  resModel, di.getName(), machine.getMachineName());
//                    unassignedResources.remove(di);
//                    if (!assignedResources.containsKey(machine)) {
//                        assignedResources.put(machine, new LinkedList<>());
//                    }
//                    assignedResources.get(machine).add(di);
//                    _logger.trace("Exiting %s true", fn);
//                    return true;
//                }
//            }
//        }
//
//        _logger.trace("Exiting %s false", fn);
//        return false;
//    }
//
//    /**
//     * Moves a DeviceInfo object of the requested ResModel, from the unassigned list
//     * to the assigned list for the given machine.
//     * This refers to _unassignedResources and _assignedResources containers.
//     * @param assignedResources map of resources which are assigned to machines
//     * @param unassignedResources collection of resources which are *not* assigned to machines
//     * @param genType indicates the general type of device we want to re-assign
//     * @param machine machine to which the device info is assigned
//     * @return true if we made the re-assignment, else false
//     */
//    private boolean moveUnassignedDeviceInfoToMachine(
//        final Map<Machine, LinkedList<DeviceInfo>> assignedResources,
//        final List<DeviceInfo> unassignedResources,
//        final LiqidGeneralType genType,
//        final Machine machine
//    ) {
//        var fn = "moveUnassignedDeviceInfoToMachine";
//        _logger.trace("Entering %s with genType=%s machine=%s", fn, genType, machine);
//
//        for (var di : unassignedResources) {
//            if (LiqidGeneralType.fromDeviceType(di.getDeviceInfoType()) == genType) {
//                _logger.trace("Assigning %s device %s to %s", genType, di.getName(), machine.getMachineName());
//                unassignedResources.remove(di);
//                if (!assignedResources.containsKey(machine)) {
//                    assignedResources.put(machine, new LinkedList<>());
//                }
//                assignedResources.get(machine).add(di);
//                _logger.trace("Exiting %s true", fn);
//                return true;
//            }
//        }
//
//        _logger.trace("Exiting %s false", fn);
//        return false;
//    }

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

//        initK8sClient();
//
//        // If there is no linkage, tell the user and stop
//        if (!hasLinkage()) {
//            throw new ConfigurationException("No linkage exists from this Kubernetes Cluster to the Liqid Cluster.");
//        }
//
//        getLiqidLinkage();
//        initLiqidClient();
//        loadLiqidInventory();
//
//        var groupId = _liqidInventory._groupsByName.get(_liqidGroupName).getGroupId();
//        var currentLayout = ClusterLayout.createFromInventory(_liqidInventory, groupId);
//        System.out.println("Current Layout:");
//        currentLayout.show("| ");
//
//        var desiredLayout = createDesiredLayout();
//        if (desiredLayout == null) {
//            throw new ConfigurationDataException("Various configuration problems exist - processing will not continue.");
//        }
//        System.out.println("Desired Layout:");
//        desiredLayout.show("| ");
//
//        if (!checkDesiredLayout(currentLayout, desiredLayout)) {
//            throw new ConfigurationDataException("Various configuration problems exist - processing will not continue.");
//        }
//
//        var plan = createPlan(desiredLayout);

        var plan = new Plan();

        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }
}
