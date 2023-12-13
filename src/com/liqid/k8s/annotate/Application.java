/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.annotate;

import com.bearsnake.k8sclient.ConfigMapPayload;
import com.bearsnake.k8sclient.K8SClient;
import com.bearsnake.k8sclient.K8SHTTPError;
import com.bearsnake.k8sclient.K8SJSONError;
import com.bearsnake.k8sclient.K8SRequestError;
import com.bearsnake.k8sclient.Node;
import com.bearsnake.k8sclient.SecretPayload;
import com.bearsnake.klog.FileWriter;
import com.bearsnake.klog.Level;
import com.bearsnake.klog.LevelMask;
import com.bearsnake.klog.Logger;
import com.bearsnake.klog.PrefixEntity;
import com.bearsnake.klog.StdOutWriter;
import com.liqid.k8s.exceptions.ConfigurationDataException;
import com.liqid.k8s.exceptions.ConfigurationException;
import com.liqid.sdk.DeviceInfo;
import com.liqid.sdk.DeviceStatus;
import com.liqid.sdk.Group;
import com.liqid.sdk.LiqidClient;
import com.liqid.sdk.LiqidClientBuilder;
import com.liqid.sdk.LiqidException;
import com.liqid.sdk.Machine;
import com.liqid.sdk.ManagedEntity;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.stream.Collectors;

import static com.liqid.k8s.Constants.K8S_ANNOTATION_KEYS;
import static com.liqid.k8s.Constants.K8S_ANNOTATION_PREFIX;
import static com.liqid.k8s.Constants.K8S_CONFIG_MAP_GROUP_NAME_KEY;
import static com.liqid.k8s.Constants.K8S_CONFIG_MAP_IP_ADDRESS_KEY;
import static com.liqid.k8s.Constants.K8S_CONFIG_NAME;
import static com.liqid.k8s.Constants.K8S_CONFIG_NAMESPACE;
import static com.liqid.k8s.Constants.K8S_SECRET_CREDENTIALS_KEY;
import static com.liqid.k8s.Constants.K8S_SECRET_NAME;
import static com.liqid.k8s.Constants.K8S_SECRET_NAMESPACE;
import static com.liqid.k8s.Constants.LIQID_SDK_LABEL;

public class Application {

    static final String LABEL_COMMAND = "label";
    static final String LINK_COMMAND = "link";
    static final String NODES_COMMAND = "nodes";
    static final String RESOURCES_COMMAND = "resources";
    static final String UNLABEL_COMMAND = "unlabel";
    static final String UNLINK_COMMAND = "unlink";
    public static final String LOGGER_NAME = "Annotate";
    public static final String LOG_FILE_NAME = "liq-annotation.log";

    // If _logging is true, we do extensive logging to a log file. If false, only errors to stdout.
    private boolean _logging = false;

    private String _command;
    private String _proxyURL;
    private int _timeoutInSeconds = 300;

    private String _liqidAddress;
    private String _liqidGroupName;
    private String _liqidPassword;
    private String _liqidUsername;
    private boolean _force = false;
    private Logger _logger;

    private K8SClient _k8sClient;
    private LiqidClient _liqidClient;

    Application setCommand(final String value) { _command = value; return this; }
    Application setLiqidAddress(final String value) { _liqidAddress = value; return this; }
    Application setLiqidGroupName(final String value) { _liqidGroupName = value; return this; }
    Application setLiqidPassword(final String value) { _liqidPassword = value; return this; }
    Application setLiqidUsername(final String value) { _liqidUsername = value; return this; }
    Application setForce(final boolean value) { _force = value; return this; }
    Application setLogging(final boolean flag) { _logging = flag; return this; }
    Application setProxyURL(final String value) { _proxyURL = value; return this; }
    Application setTimeoutInSeconds(final int value) { _timeoutInSeconds = value; return this; }

    // ------------------------------------------------------------------------
    // helper functions
    // ------------------------------------------------------------------------

//    private void addDevicesToMachine(
//        final Integer groupId,
//        final Integer machId,
//        final ResourceType resType,
//        final Integer count
//    ) throws ProcessingException, LiqidException {
//        var fn = "addDevicesToMachine";
//        _logger.atTrace().log("Entering {} groupId={} machId={} resType={} count={}",
//                              fn, groupId, machId, resType, count);
//
//        var preDevs = _liqidClient.getUnattachedDevicesForGroup(resType._queryType, groupId);
//        if (preDevs.size() < count) {
//            var msg = String.format("Internal error - we have %d %s devices but we need %d",
//                                    preDevs.size(), resType._tag, count);
//            var t = new ProcessingException(msg);
//            _logger.throwing(t);
//            throw t;
//        }
//
//        for (int dx = 0; dx < count; ++dx) {
//            var preDev = preDevs.pop();
//            var dev = _ContextForUpdate._allDevicesStatusByName.get(preDev.getDeviceName());
//            _liqidClient.addDeviceToMachine(dev.getDeviceId(), groupId, machId);
//        }
//
//        _logger.atTrace().log("{} returning", fn);
//    }

//    /**
//     * Checks aggregate variances (all variances added together per resource type)
//     * comparing them to the number of free pool or group free pool devices to see
//     * if we are over-committed.
//     * @param groupId Identifier of the kubernetes Liqid Cluster Group
//     * @return true if we are okay, else false.
//     */
//    private boolean checkAvailability(
//        final Integer groupId,
//        final Map<String, Map<ResourceType, Integer>> resourceVariances
//    ) throws LiqidException {
//        var fn = "checkAvailability";
//        _logger.atTrace().log("Entering {} groupId={} resVars={}", fn, groupId, resourceVariances);
//
//        // aggregateVariances is a collection mapping each resource type to the aggregate
//        // variance across all machines in the machines-by-name collection.
//        var aggregateVariances = Arrays.stream(ResourceType.values())
//                                       .collect(Collectors.toMap(et -> et, et -> 0, (a, b) -> b, HashMap::new));
//
//        for (var entry : resourceVariances.entrySet()) {
//            var subMap = entry.getValue();
//            for (var subEntry : subMap.entrySet()) {
//                var resType = subEntry.getKey();
//                var resVar = subEntry.getValue();
//                var newValue = aggregateVariances.get(resType) + resVar;
//                aggregateVariances.put(resType, newValue);
//            }
//        }
//
//        // availableResources is a collection mapping each resource type to the total number
//        // of available resources of that type, where 'available' means the device is
//        // either in the system pool or in the k8s group free pool.
//        var availableResources =
//            Arrays.stream(ResourceType.values())
//                  .collect(Collectors.toMap(resType -> resType, resType -> 0, (a, b) -> b, HashMap::new));
//
//        var freeGroupDevs = _liqidClient.getUnattachedDevicesForGroup(null, groupId);
//        for (var freeDev : freeGroupDevs) {
//            var resType = preDeviceTypeToResourceType(freeDev.getPreDeviceType());
//            var newValue = availableResources.get(resType) + 1;
//            availableResources.put(resType, newValue);
//        }
//
//        availableResources.put(ResourceType.RT_FPGA,
//                               availableResources.get(ResourceType.RT_FPGA) + _liqidClient.getFreeFPGADevicesStatus().size());
//        availableResources.put(ResourceType.RT_GPU,
//                               availableResources.get(ResourceType.RT_GPU) + _liqidClient.getFreeGPUDevicesStatus().size());
//        availableResources.put(ResourceType.RT_MEMORY,
//                               availableResources.get(ResourceType.RT_MEMORY) + _liqidClient.getFreeMemoryDevicesStatus().size());
//        availableResources.put(ResourceType.RT_LINK,
//                               availableResources.get(ResourceType.RT_LINK) + _liqidClient.getFreeNetworkDevicesStatus().size());
//        availableResources.put(ResourceType.RT_TARGET,
//                               availableResources.get(ResourceType.RT_TARGET) + _liqidClient.getFreeStorageDevicesStatus().size());
//
//        var result = true;
//        for (var var : aggregateVariances.entrySet()) {
//            var resType = var.getKey();
//            var wanted = var.getValue();
//            if (wanted > 0) {
//                var have = availableResources.get(resType);
//                if (wanted > have) {
//                    var msg = String.format("Insufficient %s devices - want %d, have %d", resType._tag, wanted, have);
//                    System.out.println(msg);
//                    result = false;
//                }
//            }
//        }
//
//        _logger.atTrace().log("{} returning {}", fn, result);
//        return result;
//    }

//    /**
//     * Deploys the given layout to the Liqid Client.
//     * For an initial config, the given groupId is null and we create a group.
//     * For an expansion, the existing groupId is provided as a parameter.
//     */
//    private void deployLayout(
//        final Layout layout,
//        final Integer existingGroupId
//    ) throws ProcessingException, LiqidException {
//        var fn = "deployLayout";
//        _logger.trace(String.format("Entering %s layout=%s groupId=%d", fn, layout, existingGroupId));
//
//        Integer groupId;
//        if (existingGroupId == null) {
//            // Create kubernetes group
//            var liqidGroup = _liqidClient.createGroup(LIQID_K8S_GROUP_NAME);
//            groupId = liqidGroup.getGroupId();
//        } else {
//            groupId = existingGroupId;
//        }
//
//        // Move everything we're going to attach to the eventual machines, to the new group.
//        try {
//            _liqidClient.groupPoolEdit(groupId);
//            for (var machInfo : layout._machineInfos) {
//                for (var devStat : machInfo.getAllResources()) {
//                    System.out.printf("Adding device %s to group %d...\n", devStat.getName(), groupId);
//                    _liqidClient.addDeviceToGroup(devStat.getDeviceId(), groupId);
//                }
//            }
//            _liqidClient.groupPoolDone(groupId);
//        } catch (LiqidException lex) {
//            try {
//                _liqidClient.cancelGroupPoolEdit(groupId);
//            } catch (LiqidException lex2) {
//                // can't do anything about it.
//            }
//            throw lex;
//        }
//
//        // Create the various machines and move resources to them.
//        for (var machInfo : layout._machineInfos) {
//            var editing = false;
//            var machineId = 0;
//            try {
//                var liqidMachine = _liqidClient.createMachine(groupId, machInfo.getMachineName());
//                machineId = liqidMachine.getMachineId();
//                _liqidClient.editFabric(machineId);
//                editing = true;
//                for (var devStat : machInfo.getAllResources()) {
//                    System.out.printf("Adding device %s to machine %s...\n", devStat.getName(), machInfo.getMachineName());
//                    _liqidClient.addDeviceToMachine(devStat.getDeviceId(), groupId, machineId);
//                }
//
//                _liqidClient.reprogramFabric(machineId);
//                if (machInfo.getResources(ResourceType.RT_GPU).size() > 1) {
//                    System.out.printf("Enabling P2P for machine %s...\n", machInfo.getMachineName());
//                    _liqidClient.setP2PEnabled(machineId, P2PType.ON);
//                }
//            } catch (LiqidException lex) {
//                if (editing) {
//                    try {
//                        _liqidClient.cancelEditFabric(machineId);
//                    } catch (LiqidException lex2) {
//                        // give up
//                    }
//                }
//                throw lex;
//            }
//        }
//
//        _logger.trace(String.format("%s returning", fn));
//    }
//
//    /**
//     * Produces a map of machine names to maps of resource types to the variance in resource counts
//     * for that resource type, for that machine.
//     * i.e., value['Machine-Node2'][RT_GPU] == variance of GPUs for Machine-Node2.
//     * If the variance is positive, we need to add {n} GPUs to the machine, where {n} == |variance|.
//     * If negative, we need to remove {n} GPUs from the machine, where {n} == |variance|.
//     * If any particular machine has no variances, it will not have an entry in the map.
//     */
//    private Map<String, Map<ResourceType, Integer>> getResourceVariances(
//        final K8SConfiguration configuration
//    ) throws ProcessingException {
//        var fn = "getResourceVariances";
//        _logger.trace(String.format("Entering %s config=%s", fn, configuration));
//
//        var result = new HashMap<String, Map<ResourceType, Integer>>();
//        for (var configNode : configuration.getNodes()) {
//            // Create a ResourceType-keyed map which contains the specified counts
//            // of the resources which the config file wants assigned to each k8s node.
//            // A null value indicates that the count is not specified.
//            var requestedCounts = new HashMap<ResourceType, Integer>();
//            requestedCounts.put(ResourceType.RT_FPGA, configNode._specifiedFPGACount);
//            requestedCounts.put(ResourceType.RT_GPU, configNode._specifiedGPUCount);
//            requestedCounts.put(ResourceType.RT_MEMORY, configNode._specifiedMemoryCount);
//            requestedCounts.put(ResourceType.RT_LINK, configNode._specifiedLinkCount);
//            requestedCounts.put(ResourceType.RT_TARGET, configNode._specifiedSSDCount);
//
//            // Find the Liqid Machine object which corresponds to this node,
//            // then get the list of devices (aka resources) currently assigned to this machine.
//            var machineName = String.format("%s-%s", LIQID_K8S_GROUP_NAME, configNode._k8sNodeName);
//
//            // For this machine, determine the device count variance per resource type.
//            // variance > 0 means we need more resources; variance < 0 means we need fewer.
//            var actualCounts = Arrays.stream(ResourceType.values())
//                                     .collect(Collectors.toMap(et -> et, et -> 0, (a, b) -> b, HashMap::new));
//            for (var dev : _ContextForUpdate._preDevicesByMachineName.get(machineName)) {
//                var preDevType = dev.getPreDeviceType();
//                var resType = preDeviceTypeToResourceType(preDevType);
//                actualCounts.put(resType, actualCounts.get(resType) + 1);
//            }
//
//            // build map of resource types -> variances for the machine,
//            // but only post it to the result if there is at least one non-zero variance.
//            var hasVariance = false;
//            var machineVariances = new HashMap<ResourceType, Integer>();
//            for (var resType : ResourceType.values()) {
//                var wanted = requestedCounts.get(resType);
//                var actual = actualCounts.get(resType);
//                if (wanted != null) {
//                    // wanted count is specified; variance is wanted - actual
//                    machineVariances.put(resType, wanted - actual);
//                    hasVariance = true;
//                } else if (actual > 0) {
//                    // wanted count is *not* specified, but the machine has resources -
//                    // therefore, variance is (0 - resource count).
//                    // Note that if wanted is unspecified and the machine has no resources,
//                    // then it does not have a variance for this resource type.
//                    // This rule is in place so that we can count this device's resources as available for
//                    // re-allocation in order for the reconfigure results to match the initial config results
//                    // for the current Liqid Cluster inventory given the same config file for either case.
//                    machineVariances.put(resType, -actual);
//                    hasVariance = true;
//                }
//            }
//
//            if (hasVariance) {
//                result.put(machineName, machineVariances);
//            }
//        }
//
//        _logger.trace(String.format("%s returning %s", fn, result));
//        return result;
//    }

//    /**
//     * Creates a LiqidClient object, configures it for logging, and logs in.
//     */
//    private void makeLiqidClient() throws LiqidException {
//        var fn = "makeLiqidClient";
//        _logger.atTrace().log("Entering {}", fn);
//
//        _liqidClient = new LiqidClientBuilder().setHostAddress(_directorAddress)
//                                               .setTimeoutInSeconds(_timeoutInSeconds)
//                                               .build();
//        if (_logging) {
//            LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
//            Configuration config = ctx.getConfiguration();
//            LoggerConfig loggerConfig = config.getLoggerConfig(_liqidClient.getLogger().getName());
//            loggerConfig.setLevel(Level.TRACE);
//            ctx.updateLoggers();
//        }
//
//        _liqidClient.login("LiqidK8SDeploy", _directorUsername, _directorPassword);
//        _logger.atTrace().log("{} returning", fn);
//    }

//    /**
//     * Converts a LiqidSDK pre-device type to a ResourceType
//     */
//    private static ResourceType preDeviceTypeToResourceType(
//        final PreDeviceType preDeviceType
//    ) {
//        return switch (preDeviceType) {
//            case GPU -> ResourceType.RT_GPU;
//            case TARGET -> ResourceType.RT_TARGET;
//            case LINK, FIBER_CHANNEL, INFINIBAND -> ResourceType.RT_LINK;
//            case FPGA -> ResourceType.RT_FPGA;
//            case COMPUTE -> ResourceType.RT_CPU;
//            case MEMORY -> ResourceType.RT_MEMORY;
//            case FABRIC_CHIP -> null; // should not get this one
//        };
//    }
//
//    /**
//     * Performs the actual reconfiguration
//     */
//    private void reconfigure(
//        final Integer groupId,
//        final Map<String, Map<ResourceType, Integer>> resourceVariances
//    ) throws ProcessingException, LiqidException, K8SException {
//        var fn = "reconfigure";
//        _logger.trace(String.format("Entering %s groupId=%d resVars=%s", fn, groupId, resourceVariances));
//
//        var kubeClient = new K8SClient(_proxyURL);
//        kubeClient.enableLogging(true);
//
//        var group1 = new LinkedList<String>();
//        var group2 = new LinkedList<String>();
//
//        // Move all system free resources to the group free pool, even if we end up not assigning all of them.
//        // It's... easier this way.
//        List<DeviceStatus> devs = new LinkedList<>();
//        devs.addAll(_liqidClient.getFreeFPGADevicesStatus());
//        devs.addAll(_liqidClient.getFreeGPUDevicesStatus());
//        devs.addAll(_liqidClient.getFreeMemoryDevicesStatus());
//        devs.addAll(_liqidClient.getFreeNetworkDevicesStatus());
//        devs.addAll(_liqidClient.getFreeStorageDevicesStatus());
//        if (devs.size() > 0) {
//            System.out.printf("Moving %d device(s) to the k8s Liqid Cluster Croup...\n", devs.size());
//            _liqidClient.groupPoolEdit(groupId);
//            try {
//                for (var dev : devs) {
//                    _liqidClient.addDeviceToGroup(dev.getDeviceId(), groupId);
//                }
//            } finally {
//                _liqidClient.groupPoolDone(groupId);
//            }
//        }
//
//        // Create two lists - group1 contains machine names of all machines which are losing resources
//        // of at least one resource type. group2 contains machine names of all machines which are gaining
//        // resources of at least one resource type. Note that any particular machine can be in either,
//        // both, or neither groups.
//        for (var entry : resourceVariances.entrySet()) {
//            var machName = entry.getKey();
//            var machVariances = entry.getValue();
//            var needsMore = false;
//            var needsFewer = false;
//            for (var varEntry : machVariances.entrySet()) {
//                var resVariance = varEntry.getValue();
//                if (resVariance > 0) {
//                    needsMore = true;
//                } else if (resVariance < 0) {
//                    needsFewer = true;
//                }
//            }
//
//            if (needsFewer) {
//                group1.add(machName);
//            }
//            if (needsMore) {
//                group2.add(machName);
//            }
//        }
//
//        // Remove resources from machines as appropriate. We do this first so that we
//        // have adequate free resources to add to machines in the second step.
//        for (var machName : group1) {
//            var nodeName = _ContextForUpdate._nodeNamesByMachineName.get(machName);
//            System.out.printf("Draining %s...\n", nodeName);
//            kubeClient.cordonNode(nodeName);
//            kubeClient.evictPodsForNode(nodeName, true);
//
//            var machId = _ContextForUpdate._liqidMachinesByName.get(machName).getMachineId();
//            _liqidClient.editFabric(machId);
//            var resMap = resourceVariances.get(machName);
//            for (var entry : resMap.entrySet()) {
//                var resType = entry.getKey();
//                var variance = entry.getValue();
//                try {
//                    if (variance < 0) {
//                        var count = Math.abs(variance);
//                        System.out.printf("Removing %d %s device(s) from machine %s", count, resType._tag, machName);
//                        removeDevicesFromMachine(groupId, machId, resType, count);
//                    }
//                } catch (LiqidException lex) {
//                    try {
//                        _liqidClient.cancelReprogramFabric(machId);
//                    } catch (LiqidException lex2) {
//                        // can't do anything about it
//                    }
//                    throw lex;
//                }
//            }
//            _liqidClient.reprogramFabric(machId);
//
//            System.out.printf("Uncordoning %s...\n", nodeName);
//            kubeClient.uncordonNode(nodeName);
//        }
//
//        // Add resources from machines as appropriate.
//        for (var machName : group2) {
//            var nodeName = _ContextForUpdate._nodeNamesByMachineName.get(machName);
//            System.out.printf("Draining %s...\n", nodeName);
//            kubeClient.cordonNode(nodeName);
//            kubeClient.evictPodsForNode(nodeName, true);
//
//            var machId = _ContextForUpdate._liqidMachinesByName.get(machName).getMachineId();
//            _liqidClient.editFabric(machId);
//            var resMap = resourceVariances.get(machName);
//            for (var entry : resMap.entrySet()) {
//                var resType = entry.getKey();
//                var variance = entry.getValue();
//                try {
//                    if (variance > 0) {
//                        System.out.printf("Attaching %d %s device(s) from machine %s", variance, resType._tag, machName);
//                        addDevicesToMachine(groupId, machId, resType, variance);
//                    }
//                } catch (LiqidException lex) {
//                    try {
//                        _liqidClient.cancelReprogramFabric(machId);
//                    } catch (LiqidException lex2) {
//                        // can't do anything about it
//                    }
//                    throw lex;
//                }
//            }
//            _liqidClient.reprogramFabric(machId);
//            var gpus = _liqidClient.getDevices(DeviceQueryType.GPU, groupId, machId);
//            if (gpus.size() > 1) {
//                System.out.printf("Enabling P2P for machine %s...\n", machName);
//                _liqidClient.setP2PEnabled(machId, P2PType.ON);
//            }
//
//            System.out.printf("Uncordoning %s...\n", nodeName);
//            kubeClient.uncordonNode(nodeName);
//        }
//
//        // Move whatever resources remain in the group free pool, back out to the system.
//        var allDevs = _liqidClient.getAllDevicesStatus();
//        var devStatsByName = allDevs.stream().collect(Collectors.toMap(DeviceStatus::getName,
//                                                                       devStat -> devStat,
//                                                                       (a, b) -> b,
//                                                                       HashMap::new));
//
//        var preDevices = _liqidClient.getUnattachedDevicesForGroup(null, groupId);
//        if (preDevices.size() > 0) {
//            _liqidClient.groupPoolEdit(groupId);
//            for (var dev : preDevices) {
//                try {
//                    var devStat = devStatsByName.get(dev.getDeviceName());
//                    System.out.printf("Removing %s device %s from group...\n",
//                                      devStat.getDeviceType(),
//                                      devStat.getName());
//                    _liqidClient.removeDeviceFromGroup(devStat.getDeviceId(), groupId);
//                } catch (Exception ex) {
//                    // nothing to be done, just keep going
//                }
//            }
//            _liqidClient.groupPoolDone(groupId);
//        }
//
//        _logger.trace(String.format("%s returning", fn));
//    }

//    private void removeDevicesFromMachine(
//        final Integer groupId,
//        final Integer machId,
//        final ResourceType resType,
//        final Integer count
//    ) throws ProcessingException, LiqidException {
//        var fn = "removeDevicesFromMachine";
//        _logger.trace(String.format("Entering %s groupId=%d machId=%d resType=%s count=%d",
//                                    fn, groupId, machId, resType, count));
//
//        var preDevs = _liqidClient.getDevices(resType._queryType, groupId, machId);
//        if (preDevs.size() < count) {
//            var msg = String.format("Internal error - we have %d %s devices but we need %d",
//                                    preDevs.size(), resType._tag, count);
//            var t = new ProcessingException(msg);
//            _logger.throwing(t);
//            throw t;
//        }
//
//        for (int dx = 0; dx < count; ++dx) {
//            var preDev = preDevs.pop();
//            var dev = _ContextForUpdate._allDevicesStatusByName.get(preDev.getDeviceName());
//            _liqidClient.removeDeviceFromMachine(dev.getDeviceId(), groupId, machId);
//        }
//
//        _logger.trace(String.format("%s returning", fn));
//    }
//
//    /**
//     * Compares results of the wanted configuration with the existing layout
//     * to determine if there are any variances between the general node naming
//     * and the host deployments.
//     * Returns a list of user-readable strings detailing the variances found...
//     * if there are none, then the resulting list is empty.
//     */
//    private List<String> resolveCPUsAndMachines(
//        final K8SConfiguration configuration,
//        final Layout existingLayout
//    ) {
//        var fn = "resolveCPUsAndMachines";
//        _logger.trace(String.format("Entering %s config=%s, layout=%s", fn, configuration, existingLayout));
//
//        var result = new LinkedList<String>();
//
//        var pcpuNameToNodeName = new HashMap<String, String>();
//        var nodeNameToPcpuName = new HashMap<String, String>();
//        for (var configNode : configuration.getNodes()) {
//            var pcpuName = configNode._liqidPCPUName;
//            var nodeName = configNode._k8sNodeName;
//            pcpuNameToNodeName.put(pcpuName, nodeName);
//            nodeNameToPcpuName.put(nodeName, pcpuName);
//        }
//
//        var machineNameToCPUStatus = new HashMap<String, DeviceStatus>();
//        for (var machInfo : existingLayout._machineInfos) {
//            machineNameToCPUStatus.put(machInfo.getMachineName(), machInfo.getComputeDeviceStatus());
//        }
//
//        var machineNamePrefix = LIQID_K8S_GROUP_NAME + "-";
//        for (var entry : pcpuNameToNodeName.entrySet()) {
//            var pcpuName = entry.getKey();
//            var nodeName = entry.getValue();
//            var expectedMachineName = machineNamePrefix + nodeName;
//            if (!machineNameToCPUStatus.containsKey(expectedMachineName)) {
//                var msg = String.format("Machine name '%s' composed from information in config is not found in layout.",
//                                        expectedMachineName);
//                result.add(msg);
//            } else {
//                var cpuStat = machineNameToCPUStatus.get(expectedMachineName);
//                if (!pcpuName.equals(cpuStat.getName())) {
//                    var msg = String.format("PCPU name '%s' from config does not match machine '%s' pcpu name '%s'.",
//                                            pcpuName, expectedMachineName, cpuStat.getName());
//                    result.add(msg);
//                }
//            }
//        }
//
//        for (var entry : machineNameToCPUStatus.entrySet()) {
//            var machName = entry.getKey();
//            var cpuStat = entry.getValue();
//            if (!machName.startsWith(machineNamePrefix)) {
//                var msg = String.format("Machine name '%s' from Liqid config does not start with '%s'.",
//                                        machName,
//                                        LIQID_K8S_GROUP_NAME);
//                result.add(msg);
//                continue;
//            }
//
//            var expectedPCPUName = cpuStat.getName();
//            var expectedNodeName = machName.substring(LIQID_K8S_GROUP_NAME.length() + 1);
//            if (!pcpuNameToNodeName.containsKey(expectedPCPUName)) {
//                var msg = String.format("For machine '%s', node name '%s' si not in the requested configuration.",
//                                        machName,
//                                        expectedPCPUName);
//                result.add(msg);
//            } else {
//                if (!nodeNameToPcpuName.containsKey(expectedNodeName)
//                    || !nodeNameToPcpuName.get(expectedNodeName).equals(expectedPCPUName)) {
//                    var msg = String.format("For machine '%s', node '%s' is not associated with port '%s'.",
//                                            machName,
//                                            expectedNodeName,
//                                            expectedPCPUName);
//                    result.add(msg);
//                }
//            }
//
//            if (!nodeNameToPcpuName.containsKey(expectedNodeName)) {
//                var msg = String.format("For machine '%s', node name '%s' is not in the requested configuration.",
//                                        machName,
//                                        expectedNodeName);
//                result.add(msg);
//            } else {
//                if (!pcpuNameToNodeName.containsKey(expectedPCPUName)
//                    || !pcpuNameToNodeName.get(expectedPCPUName).equals(expectedNodeName)) {
//                    var msg = String.format("For machine '%s', port '%s' is not associated with node '%s'.",
//                                            machName,
//                                            expectedPCPUName,
//                                            expectedNodeName);
//                    result.add(msg);
//                }
//            }
//        }
//
//        _logger.trace(String.format("%s returning %s", fn, result));
//        return result;
//    }

    // ------------------------------------------------------------------------
    // top-level command handlers
    // ------------------------------------------------------------------------

//    private void doConfig() throws ScriptException, LiqidException, K8SException {
//        var fn = "doConfig";
//        _logger.atTrace().log("Entering {}", fn);
//
//        makeLiqidClient();
//        makeK8SClient();
//
//        try {
//            // Ensure that no Kubernetes group exists, and that at least one node is in the free pool
//            var groups = _liqidClient.getGroups();
//            if (!groups.isEmpty() && !_clearContext && !_showMode) {
//                var t = new SetupException("Cannot run config action without a clean configuration");
//                _logger.throwing(t);
//                throw t;
//            }
//
//            System.out.println();
//            System.out.println("Reading Liqid Cluster Configuration...");
//            var liqidConfig = LiqidConfiguration.create(_liqidClient);
//            liqidConfig.show("| ");
//
//            System.out.println();
//            System.out.println("Reading K8S Configuration...");
//            var k8sConfig = new K8SConfiguration(_logger);
//            k8sConfig.load(_k8sClient);
//            k8sConfig.display("| ");
//
//            var layout = Layout.createForInitialConfiguration(k8sConfig, liqidConfig, _logger);
//            System.out.println();
//            System.out.println("Target Layout:");
//            layout.show("| ");
//
//            var plan = Plan.createForInitialConfiguration(liqidConfig, _clearContext, LIQID_K8S_GROUP_NAME, layout);
//            plan.show();
//
//            if (!_showMode) {
//                plan.execute(_k8sClient, _liqidClient, _logger);
//            }
//        } finally {
//            _liqidClient.logout();
//        }
//
//        _logger.atTrace().log("{} returning", fn);
//    }
//
//    private void doExpand() throws ScriptException, LiqidException, K8SException {
//        var fn = "doExpand";
//        _logger.atTrace().log("Entering {}", fn);
//
//        // This is to be used when the administrator has already used the config command at least once.
//        // There are precautions we could take, and may do so in the future.
//        // For now, we just raid the liqid config for all the compute devices identified by liqid annotations
//        // in k8s which are in the system free pool so we can get them into the k8s group, then create
//        // machines for all the nodes in the k8s group which are not already in a machine.
//        if (_clearContext) {
//            var t = new SetupException("Will not clear configuration for expand command");
//            _logger.throwing(t);
//            throw t;
//        }
//
//        makeLiqidClient();
//        makeK8SClient();
//
//        try {
//            // find the kubernetes group
//            // account for temporary API bug - should give us a 404 which raises an exception.
//            var groupId = _liqidClient.getGroupIdByName(LIQID_K8S_GROUP_NAME);
//            if (groupId == 0) {
//                var t = new SetupException(String.format("Kubernetes group '%s' does not exists", LIQID_K8S_GROUP_NAME));
//                _logger.throwing(t);
//                throw t;
//            }
//
//            System.out.println();
//            System.out.println("Reading Liqid Cluster Configuration...");
//            var liqidConfig = LiqidConfiguration.create(_liqidClient);
//            liqidConfig.show("| ");
//
//            System.out.println();
//            System.out.println("Reading K8S Configuration...");
//            var k8sConfig = new K8SConfiguration(_logger);
//            k8sConfig.load(_k8sClient);
//            k8sConfig.display("| ");
//
//            // make a plan and execute it.
//            var currentLayout = Layout.createExisting(k8sConfig, liqidConfig, groupId, _logger);
//            System.out.println();
//            System.out.println("Existing Layout:");
//            currentLayout.show("| ");
//
//            var requestedLayout = Layout.createForAddingNodes(k8sConfig, liqidConfig, groupId, _logger);
//            System.out.println();
//            System.out.println("Updated Layout:");
//            requestedLayout.show("| ");
//
//            var plan = Plan.createForAddingNodes(currentLayout, requestedLayout);
//            plan.show();
//
//            if (!_showMode) {
//                plan.execute(_k8sClient, _liqidClient, _liqidClient.getGroup(groupId), _logger);
//            }
//        } finally {
//            _liqidClient.logout();
//        }
//
//        _logger.atTrace().log("{} returning", fn);
//    }
//
//    private void doUpdate() throws ScriptException, LiqidException, K8SException {
//        var fn = "doUpdate";
//        _logger.atTrace().log("Entering {}", fn);
//
//        if (_clearContext) {
//            var t = new SetupException("Will not clear configuration for expand command");
//            _logger.throwing(t);
//            throw t;
//        }
//
//        makeLiqidClient();
//        makeK8SClient();
//
//        try {
//            // find the kubernetes group
//            // account for temporary API bug - should give us a 404 which raises an exception.
//            var groupId = _liqidClient.getGroupIdByName(LIQID_K8S_GROUP_NAME);
//            if (groupId == 0) {
//                var t = new SetupException(String.format("Kubernetes group '%s' does not exists", LIQID_K8S_GROUP_NAME));
//                _logger.throwing(t);
//                throw t;
//            }
//
//            System.out.println();
//            System.out.println("Reading Liqid Cluster Configuration...");
//            var liqidConfig = LiqidConfiguration.create(_liqidClient);
//            liqidConfig.show("| ");
//
//            System.out.println();
//            System.out.println("Reading K8S Configuration...");
//            var k8sConfig = new K8SConfiguration(_logger);
//            k8sConfig.load(_k8sClient);
//            k8sConfig.display("| ");
//
//            // make a plan and execute it.
//            var currentLayout = Layout.createExisting(k8sConfig, liqidConfig, groupId, _logger);
//            System.out.println();
//            System.out.println("Existing Layout:");
//            currentLayout.show("| ");
//
//            var requestedLayout = Layout.createForInitialConfiguration(k8sConfig, liqidConfig, _logger);
//            System.out.println();
//            System.out.println("Updated Layout:");
//            requestedLayout.show("| ");
//
//            var plan = Plan.createForReconfiguration(currentLayout, requestedLayout);
//            plan.show();
//
//            if (!_showMode) {
//                plan.execute(_k8sClient, _liqidClient, _liqidClient.getGroup(groupId), _logger);
//            }
//        } finally {
//            _liqidClient.logout();
//        }
//
//        _logger.trace(String.format("%s returning", fn));
//    }

    // ------------------------------------------------------------------------
    // function helpers
    // ------------------------------------------------------------------------

    // Retrieves the stored Liqid linkages from the k8s database
    private boolean getLiqidLinkage(
    ) throws ConfigurationDataException, K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = "getLiqidLinkage";
        _logger.trace("Entering %s", fn);

        try {
            var cfgMap = _k8sClient.getConfigMap(K8S_CONFIG_NAMESPACE, K8S_CONFIG_NAME);
            _liqidAddress = cfgMap.data.get(K8S_CONFIG_MAP_IP_ADDRESS_KEY);
            _liqidGroupName = cfgMap.data.get(K8S_CONFIG_MAP_GROUP_NAME_KEY);
        } catch (K8SHTTPError ex) {
            if (ex.getResponseCode() == 404) {
                //  We will get a 404 if there is no linkage
                System.err.println("ERROR:No linkage configured for this Kubernetes Cluster");
                var result = false;
                _logger.trace("Exiting %s with %s", fn, result);
                return result;
            } else {
                //  Anything else is a legitimate problem.
                _logger.throwing(ex);
                throw ex;
            }
        }

        try {
            var secret = _k8sClient.getSecret(K8S_SECRET_NAMESPACE, K8S_SECRET_NAME);
            var bytes = Base64.getDecoder().decode(secret.data.get(K8S_SECRET_CREDENTIALS_KEY));
            var composite = new String(bytes);
            var split = composite.split(":");
            if (split.length > 2) {
                var ex = new ConfigurationDataException("Malformed decoded Base64 string detected while obtaining Liqid credentials");
                _logger.throwing(ex);
                throw ex;
            }
            _liqidUsername = split[0];
            _liqidPassword = split.length == 2 ? split[1] : null;
        } catch (K8SHTTPError ex) {
            if (ex.getResponseCode() == 404) {
                //  A 404 could indicate no linkage, but we wouldn't be here in that case.
                //  Thus, if we get here *now* with a 404, it simply means no credentials are configured,
                //  and that might be correct, so we just continue.
            } else {
                //  Anything else is a legitimate problem.
                _logger.throwing(ex);
                throw ex;
            }
        }

        var result = true;
        _logger.trace("Exiting %s with %s", fn, result);
        return result;
    }

    private void initLogging() throws IOException {
        var level = _logging ? Level.TRACE : Level.ERROR;
        _logger = new Logger(LOGGER_NAME);
        _logger.setLevel(level);

        _logger.addWriter(new StdOutWriter(Level.ERROR));
        if (_logging) {
            var fw = new FileWriter(new LevelMask(level), LOG_FILE_NAME, false);
            fw.addPrefixEntity(PrefixEntity.SOURCE_CLASS);
            fw.addPrefixEntity(PrefixEntity.SOURCE_METHOD);
            fw.addPrefixEntity(PrefixEntity.SOURCE_LINE_NUMBER);
            _logger.addWriter(fw);
        }
    }

    private boolean initK8sClient() {
        var fn = "initK8sClient";
        _logger.trace("Entering %s", fn);

        var result = true;
        try {
            _k8sClient = new K8SClient(_proxyURL, _logger);
        } catch (IOException ex) {
            _logger.catching(ex);
            System.err.println("ERROR:An error occurred while setting up communication with the Kubernetes cluster");
            System.err.println("     :" + ex);
            result = false;
        }

        _logger.trace("Exiting %s with %s", fn, result);
        return result;
    }

    private boolean initLiqidClient() {
        var fn = "initLiqidClient";
        _logger.trace("Entering %s", fn);

        var result = true;
        try {
            _liqidClient = new LiqidClientBuilder().setHostAddress(_liqidAddress)
                                                   .setTimeoutInSeconds(_timeoutInSeconds)
                                                   .build();
            _liqidClient.setLogger(_logger);
            if (_liqidUsername != null) {
                _liqidClient.login(LIQID_SDK_LABEL, _liqidUsername, _liqidPassword);
            }
        } catch (LiqidException ex) {
            // This is not good; however, certain client scenarios might be allowed to proceed anyway if
            // force is set, so we just log the problem and return false and let the caller figure it out.
            _logger.catching(ex);
            result = false;
        }

        _logger.trace("Exiting %s with %s", fn, result);
        return result;
    }

    // ------------------------------------------------------------------------
    // top-level command handlers
    // ------------------------------------------------------------------------

    private void doLabel() {
        var fn = LABEL_COMMAND;
        _logger.trace("Entering %s", fn);

        if (!initK8sClient()) {
            _logger.trace("Exiting %s", fn);
            return;
        }

        //  TODO

        _logger.trace("Exiting %s", fn);
    }

    private void doLink() throws K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = LINK_COMMAND;
        _logger.trace("Entering %s", fn);

        if (!initK8sClient()) {
            _logger.trace("Exiting %s", fn);
            return;
        }

        // If there is already a configMap with this cluster name...
        //      If force is set, write warning, delete the existing info, and continue
        //      Otherwise tell user it already exists, and stop
        try {
            var oldConfigMap = _k8sClient.getConfigMap(K8S_CONFIG_NAMESPACE, K8S_CONFIG_NAME);
            if (oldConfigMap != null) {
                if (!_force) {
                    System.err.println("ERROR:A link already exists between the Kubernetes Cluster and the Liqid Cluster.");
                    for (var e : oldConfigMap.data.entrySet()) {
                        System.err.printf("     : %s = %s\n", e.getKey(), e.getValue());
                    }
                    return;
                }

                System.err.println("WARNING:A link already exists between the Kubernetes Cluster and the Liqid Cluster.");
                System.err.println("       :The existing link will be deleted, and a new one created.");
                _k8sClient.deleteConfigMap(K8S_CONFIG_NAMESPACE, K8S_CONFIG_NAME);
            }
        } catch (K8SHTTPError ex) {
            //  We *should* get here with a 404. Anything other than a 404 is a Bad Thing.
            if (ex.getResponseCode() != 404) {
                throw ex;
            }
        }

        // Is there a configMap? If so, get rid of it.
        try {
            _k8sClient.deleteSecret(K8S_SECRET_NAMESPACE, K8S_SECRET_NAME);
        } catch (K8SHTTPError ex) {
            //  A 404 is okay - anything else is not
            if (ex.getResponseCode() != 404) {
                throw ex;
            }
        }

        // Now go verify that the link is correct (i.e., that we can contact the Liqid Director)
        try {
            if (!initLiqidClient()) {
                if (_force) {
                    System.err.println("WARNING:Cannot connect to the Liqid Cluster, but proceeding anyway.");
                } else {
                    System.err.println("ERROR:Cannot connect to the Liqid Cluster - stopping.");
                    return;
                }
            } else {
                // Logged in - see if the indicated group exists.
                var groups = _liqidClient.getGroups();
                boolean found = false;
                for (var group : groups) {
                    if (group.getGroupName().equals(_liqidGroupName)) {
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    if (_force) {
                        System.err.println("WARNING:Group " + _liqidGroupName + " does not exist on the Liqid Cluster and will be created");
                        _liqidClient.createGroup(_liqidGroupName);
                    } else {
                        System.err.println("ERROR:Group " + _liqidGroupName + " does not exist on the Liqid Cluster");
                        return;
                    }
                }
            }
        } catch (LiqidException ex) {
            _logger.catching(ex);
            if (_force) {
                System.err.println("WARNING:Cannot connect to Liqid Cluster - proceeding anyway due to force being set");
            } else {
                System.err.println("ERROR:Cannot connect to Liqid Cluster - stopping");
                return;
            }
        }

        // Write the configMap
        var newCfgMap = new ConfigMapPayload();
        newCfgMap.metadata.namespace = K8S_CONFIG_NAMESPACE;
        newCfgMap.metadata.name = K8S_CONFIG_NAME;
        newCfgMap.data = new HashMap<>();
        newCfgMap.data.put(K8S_CONFIG_MAP_IP_ADDRESS_KEY, _liqidAddress);
        newCfgMap.data.put(K8S_CONFIG_MAP_GROUP_NAME_KEY, _liqidGroupName);
        _k8sClient.createConfigMap(newCfgMap);

        // If there are credentials, write a secret
        if (_liqidUsername != null) {
            var newSecret = new SecretPayload();
            newSecret.metadata.namespace = K8S_SECRET_NAMESPACE;
            newSecret.metadata.name = K8S_SECRET_NAME;
            var str = _liqidUsername;
            if (_liqidPassword != null) {
                str += ":" + _liqidPassword;
            }
            newSecret.data.put(K8S_SECRET_CREDENTIALS_KEY, Base64.getEncoder().encodeToString(str.getBytes()));
            _k8sClient.createSecret(newSecret);
        }

        // All done
        if ((_liqidClient != null) && (_liqidClient.isLoggedIn())) {
            try {
                _liqidClient.logout();
            } catch (LiqidException ex) {
                System.err.println("WARNING:Failed to logout of Liqid Cluster:" + ex);
            }
        }

        _logger.trace("Exiting %s", fn);
    }

    private void doNodes() throws K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = NODES_COMMAND;
        _logger.trace("Entering %s", fn);

        if (!initK8sClient()) {
            _logger.trace("Exiting %s", fn);
            return;
        }

        try {
            System.out.println("Liqid Cluster linkage settings:");
            var configMap = _k8sClient.getConfigMap(K8S_CONFIG_NAMESPACE, K8S_CONFIG_NAME);
            for (var entry : configMap.data.entrySet()) {
                System.out.printf("  %s: %s\n", entry.getKey(), entry.getValue());
            }
        } catch (K8SHTTPError ex) {
            if (ex.getResponseCode() == 404) {
                System.out.println("  <None established>");
            } else {
                throw ex;
            }
        }

        var nodes = _k8sClient.getNodes();
        for (var node : nodes) {
            var nodeName = node.metadata.name;
            System.out.println("Node " + nodeName + ":");
            var annotations = _k8sClient.getAnnotationsForNode(node.metadata.name);
            var hasEntry = false;
            for (var entry : annotations.entrySet()) {
                if (entry.getKey().startsWith(K8S_ANNOTATION_PREFIX)) {
                    System.out.println("  " + entry.getKey() + ": " + entry.getValue());
                    hasEntry = true;
                }
            }

            if (!hasEntry) {
                System.out.println("  <Node has no liqid-specific annotations>");
            }
        }

        _logger.trace("Exiting %s", fn);
    }

    private void doResources(
    ) throws ConfigurationException, ConfigurationDataException, K8SHTTPError, K8SJSONError, K8SRequestError, LiqidException {
        var fn = RESOURCES_COMMAND;
        _logger.trace("Entering %s", fn);

        if (!initK8sClient()) {
            _logger.trace("Exiting %s", fn);
            return;
        }

        if (!getLiqidLinkage()) {
            var ex = new ConfigurationException("No linkage exists between the Kubernetes Cluster and a Liqid Cluster.");
            _logger.throwing(ex);
            throw ex;
        }

        if (!initLiqidClient()) {
            System.err.println("ERROR:Cannot connect to the Liqid Cluster");
            _logger.trace("Exiting %s", fn);
            return;
        }

        Integer groupId = null;
        Group group = null;
        try {
            groupId = _liqidClient.getGroupIdByName(_liqidGroupName);
            group = _liqidClient.getGroup(groupId);
            System.out.printf("Found Liqid Cluster group ID=%d Name=%s\n", group.getGroupId(), group.getGroupName());
        } catch (LiqidException ex) {
            // If we end up here, it's probably because the group does not exist
            System.err.println("ERROR:Liqid group '" + _liqidGroupName + "' does not exist");
            _logger.trace("Exiting %s", fn);
        }

        // Create a map of all devices in the cluster, by device name.
        // We do this so that we can look them up from the preDevice object.
        // We do it this way in order to leverage the efficiency of getting all the device status objects at once,
        // rather than looking them up one-by-one per preDevice.
        var devStats = _liqidClient.getAllDevicesStatus();
        var devStatMap = devStats.stream()
                                 .collect(Collectors.toMap(DeviceStatus::getName, dev -> dev, (a, b) -> b, HashMap::new));

        // Now do something similar with managed entities
        var mes = _liqidClient.getManagedEntities();
        var pciMap = new HashMap<String, ManagedEntity>();
        for (var me : mes) {
            var key = me.getPCIVendorId() + ":" + me.getPCIDeviceId();
            pciMap.put(key, me);
        }

        // Grab all DeviceInfo things and store them by name
        var infosList = new LinkedList<DeviceInfo>();
        infosList.addAll(_liqidClient.getComputeDeviceInfo());
        infosList.addAll(_liqidClient.getFPGADeviceInfo());
        infosList.addAll(_liqidClient.getGPUDeviceInfo());
        infosList.addAll(_liqidClient.getMemoryDeviceInfo());
        infosList.addAll(_liqidClient.getNetworkDeviceInfo());
        infosList.addAll(_liqidClient.getStorageDeviceInfo());
        var infos = new HashMap<String, DeviceInfo>();
        for (var info : infosList) {
            infos.put(info.getName(), info);
        }

        // One more map - this one stores machines by machine ID
        var machines = _liqidClient.getMachines();
        var machineMap = machines.stream()
                                 .collect(Collectors.toMap(Machine::getMachineId, mach -> mach, (a, b) -> b, HashMap::new));

        System.out.println("Resource within the group:");
        var preDevices = _liqidClient.getDevices(null, groupId, null);
        System.out.println("  ---TYPE---  --NAME--  ----ID----  --VENDOR--  --MODEL---  -------MACHINE--------  --DESCRIPTION--");
        for (var preDev : preDevices) {
            var ds = devStatMap.get(preDev.getDeviceName());
            var pciKey = ds.getPCIVendorId() + ":" + ds.getPCIDeviceId();
            var me = pciMap.get(pciKey);
            var info = infos.get(preDev.getDeviceName());

            var machStr = "<none>";
            var machId = preDev.getMachineId();
            if (machId != 0) {
                var mach = machineMap.get(machId);
                machStr = mach.getMachineName();
            }

            System.out.printf("  %-10s  %-8s  0x%08x  %-10s  %-10s  %-22s  %s\n",
                              ds.getDeviceType(),
                              ds.getName(),
                              ds.getDeviceId(),
                              me.getManufacturer(),
                              me.getModel(),
                              machStr,
                              info.getUserDescription());
        }

        System.out.println("Machines within the group:");
        System.out.println("  ----------------------  ----ID----  --CPU--");
        for (var mach : machines) {
            if (mach.getGroupId().equals(groupId)) {
                System.out.printf("  %-22s  0x%08x  %s\n", mach.getMachineName(), mach.getMachineId(), mach.getComputeName());
            }
        }

        _logger.trace("Exiting %s", fn);
    }

    private void doUnlabel() {
        var fn = UNLABEL_COMMAND;
        _logger.trace("Entering %s", fn);

        if (!initK8sClient()) {
            _logger.trace("Exiting %s", fn);
            return;
        }

        // TODO

        _logger.trace("Exiting %s", fn);
    }

    private void doUnlink() throws K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = LINK_COMMAND;
        _logger.trace("Entering %s", fn);

        if (!initK8sClient()) {
            _logger.trace("Exiting %s", fn);
            return;
        }

        // If there is no configMap for this cluster-name, tell the user and stop
        try {
            var cfgMap = _k8sClient.getConfigMap(K8S_CONFIG_NAMESPACE, K8S_CONFIG_NAME);
        } catch (K8SHTTPError ex) {
            if (ex.getResponseCode() == 404) {
                System.err.println("ERROR:No linkage exists from this Kubernetes Cluster to the Liqid Cluster.");
                return;
            } else {
                throw ex;
            }
        }

        //  If there are any worker-node annotations referring to the Liqid Cluster...
        //      if force is *not* set, tell the user and stop
        //      if force *is* set, unlabel all the relevant nodes and continue
        var workersWithAnnotations = new LinkedList<Node>();
        var nodeEntities = _k8sClient.getNodes();
        for (var node : nodeEntities) {
            var annos = _k8sClient.getAnnotationsForNode(node.getName());
            for (var key : K8S_ANNOTATION_KEYS) {
                if (annos.containsKey(key)) {
                    workersWithAnnotations.add(node);
                    break;
                }
            }
        }

        if (!workersWithAnnotations.isEmpty()) {
            var prefix1 = _force ? "WARNING" : "ERROR";
            var prefix2 = _force ? "       " : "     ";
            System.err.printf("%s:The following worker nodes have Liqid Cluster-related annotations:\n", prefix1);
            var names = workersWithAnnotations.stream().map(Node::getName).collect(Collectors.toCollection(LinkedList::new));
            System.err.printf("%s:  %s\n", prefix2, String.join(", ", names));
            if (!_force) {
                System.err.printf("%s:unlink command will not be performed unless forced.\n", prefix2);
                return;
            }

            System.err.println("%s:The nodes will be un-annotated.");
            for (var node : workersWithAnnotations) {
                for (var key : K8S_ANNOTATION_KEYS) {
                    node.metadata.annotations.remove(key);
                }
                _k8sClient.updateAnnotationsForNode(node.getName(), node.metadata.annotations);
            }
        }

        // delete the configMap and secret
        _k8sClient.deleteConfigMap(K8S_CONFIG_NAMESPACE, K8S_CONFIG_NAME);
        try {
            _k8sClient.deleteSecret(K8S_SECRET_NAMESPACE, K8S_SECRET_NAME);
        } catch (K8SHTTPError ex) {
            //  If we got a 404, it's okay and even expected it there are no credentials.
            //  Non-404 is not okay.
            if (ex.getResponseCode() != 404) {
                throw ex;
            }
        }

        _logger.trace("Exiting %s", fn);
    }

    void process() {
        try {
            initLogging();
        } catch (IOException ex) {
            System.err.println("Logging initialization failure:" + ex);
        }

        var fn = "process";
        _logger.trace("Entering %s", fn);

        try {
            switch (_command) {
                case LABEL_COMMAND -> doLabel();
                case LINK_COMMAND -> doLink();
                case NODES_COMMAND -> doNodes();
                case RESOURCES_COMMAND -> doResources();
                case UNLABEL_COMMAND -> doUnlabel();
                case UNLINK_COMMAND -> doUnlink();
            }
        } catch (ConfigurationDataException ex) {
            _logger.catching(ex);
            System.err.println("Configuration Data inconsistency(ies) prevent further processing.");
            System.err.println("Please collect logging information and contact Liqid Support.");
        } catch (ConfigurationException ex) {
            _logger.catching(ex);
            System.err.println("Configuration inconsistency(ies) prevent further processing.");
            System.err.println("Please collect logging information and contact Liqid Support.");
        } catch (K8SJSONError ex) {
            _logger.catching(ex);
            System.err.println("Something went wrong while parsing JSON data from the Kubernetes cluster.");
            System.err.println("Please collect logging information and contact Liqid Support.");
        } catch (K8SHTTPError ex) {
            _logger.catching(ex);
            var code = ex.getResponseCode();
            System.err.printf("Received unexpected %d HTTP response from the Kubernetes API server.\n", code);
            System.err.println("Please verify that you have provided the correct IP address and port information,");
            System.err.println("and that the API server (or proxy server) is up and running.");
        } catch (K8SRequestError ex) {
            _logger.catching(ex);
            System.err.println("Could not complete the request to the Kubernetes API server.");
            System.err.println("Error: " + ex.getMessage());
            System.err.println("Please verify that you have provided the correct IP address and port information,");
            System.err.println("and that the API server (or proxy server) is up and running.");
        } catch (LiqidException ex) {
            _logger.catching(ex);
            System.err.println("Could not complete the request due to an error communicating with the Liqid Cluster.");
            System.err.println("Error: " + ex.getMessage());
            System.err.println("Please verify that you have provided the correct IP address and port information,");
            System.err.println("and that the API server (or proxy server) is up and running.");
        }

        _logger.trace("Exiting %s", fn);
    }
}
