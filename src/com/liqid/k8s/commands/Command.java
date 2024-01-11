/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.commands;

import com.bearsnake.k8sclient.*;
import com.bearsnake.klog.*;
import com.liqid.k8s.*;
import com.liqid.k8s.exceptions.*;
import com.liqid.k8s.plan.Plan;
import com.liqid.k8s.plan.actions.*;
import com.liqid.sdk.*;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.liqid.k8s.Constants.*;

/**
 * Abstract class for all command handlers.
 * Most of the really useful functionality exists in this class, with the subclasses simply calling back
 * if and as necessary.
 */
public abstract class Command {

    protected static final Map<LiqidGeneralType, String> ANNOTATION_KEY_FOR_DEVICE_TYPE = new HashMap<>();
    static {
        ANNOTATION_KEY_FOR_DEVICE_TYPE.put(LiqidGeneralType.FPGA, K8S_ANNOTATION_FPGA_ENTRY);
        ANNOTATION_KEY_FOR_DEVICE_TYPE.put(LiqidGeneralType.GPU, K8S_ANNOTATION_GPU_ENTRY);
        ANNOTATION_KEY_FOR_DEVICE_TYPE.put(LiqidGeneralType.MEMORY, K8S_ANNOTATION_MEMORY_ENTRY);
        ANNOTATION_KEY_FOR_DEVICE_TYPE.put(LiqidGeneralType.LINK, K8S_ANNOTATION_LINK_ENTRY);
        ANNOTATION_KEY_FOR_DEVICE_TYPE.put(LiqidGeneralType.SSD, K8S_ANNOTATION_SSD_ENTRY);
    }

    protected final Logger _logger;
    protected final Boolean _force;
    protected final Integer _timeoutInSeconds;

    protected String _liqidAddress;
    protected String _liqidGroupName;
    protected String _liqidPassword;
    protected String _liqidUsername;
    protected String _proxyURL;

    protected K8SClient _k8sClient;
    protected LiqidClient _liqidClient;
    protected LiqidInventory _liqidInventory;

    protected Command(
        final Logger logger,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        _logger = logger;
        _force = force;
        _timeoutInSeconds = timeoutInSeconds;
    }

    /**
     * Instructs the subclass to process itself given sufficient information
     * @return a Plan if processing was able to produce one
     * @throws ConfigurationException Indicates an inconsistency in the general configuration of the Liqid Cluster
     *                                  or the Kubernetes Cluster
     * @throws ConfigurationDataException Indicates a problem in the actual liqid-supplied data stored in the
     *                                      Liqid Cluster or the Kubernetes Cluster
     * @throws InternalErrorException Indicates that some error has been detected which is likely caused by an error
     *                                  in programming
     * @throws K8SHTTPError Indicates that the Kubernetes API returned an HTTP status which we did not expect
     * @throws K8SJSONError Indicates that the Kubernetes API returned information in a format which we did not expect
     * @throws K8SRequestError Indicates some problem was encountered by the Kubernetes API library - this could be
     *                          a programming problem, or it could be something wrong in the Kubernetes API.
     * @throws LiqidException Indicates that a general error occurred while interacting with the Liqid Director API.
     */
    public abstract Plan process(
    ) throws ConfigurationException,
             ConfigurationDataException,
             InternalErrorException,
             K8SException,
             LiqidException,
             ProcessingException;

    public K8SClient getK8SClient() { return _k8sClient; }
    public LiqidClient getLiqidClient() { return _liqidClient; }

    /**
     * Creates steps to allocate resources as equally as possible among the known worker nodes.
     * This is specific to the initialize command, as it assumes that all relevant resources are
     * in the containing group, and *not* already assigned to any machines.
     */
    protected void allocateEqually(
        final Plan plan,
        final Map<DeviceStatus, Node> computeDevices,
        final Collection<DeviceStatus> resourceDevices
    ) {
        var fn = "allocateEqually";
        _logger.trace("Entering %s", fn);

        var devsByType = new HashMap<LiqidGeneralType, LinkedList<DeviceStatus>>();
        for (var dev : resourceDevices) {
            var genType = LiqidGeneralType.fromDeviceType(dev.getDeviceType());
            devsByType.computeIfAbsent(genType, k -> new LinkedList<>());
            devsByType.get(genType).add(dev);
        }

        // we're going to loop by device type, but we want to act by machine.
        // So... this loop creates a container which we'll deal with in the next paragraph.
        var layout = new HashMap<DeviceStatus, HashMap<LiqidGeneralType, Integer>>();
        for (var dbtEntry : devsByType.entrySet()) {
            var genType = dbtEntry.getKey();
            var devs = dbtEntry.getValue();
            var devCount = devs.size();
            var workerCount = computeDevices.size();

            for (var devStat : computeDevices.keySet()) {
                var resCount = devCount / workerCount;
                if (devCount % workerCount > 0) {
                    resCount++;
                }

                layout.computeIfAbsent(devStat, k -> new HashMap<>());
                layout.get(devStat).put(genType, resCount);

                workerCount--;
                devCount -= resCount;
                if ((workerCount == 0) || (devCount == 0)) {
                    break;
                }
            }
        }

        for (var entry : layout.entrySet()) {
            var devStat = entry.getKey();
            var node = computeDevices.get(devStat);
            var resMap = entry.getValue();
            var annoAction = new AnnotateNode().setNodeName(node.getName());
            var asgDevs = new LinkedList<DeviceStatus>();
            for (var resEntry : resMap.entrySet()) {
                var genType = resEntry.getKey();
                var resCount = resEntry.getValue();
                var annoKey = ANNOTATION_KEY_FOR_DEVICE_TYPE.get(genType);
                var annoValue = String.format("%d", resCount);
                annoAction.addAnnotation(annoKey, annoValue);

                var devs = devsByType.get(genType);
                while (asgDevs.size() < resCount) {
                    asgDevs.add(devs.pop());
                }
            }

            var machName = createMachineName(devStat, node);
            var asgAction = new AssignToMachine().setMachineName(machName);
            for (var asgDev : asgDevs) {
                asgAction.addDeviceName(asgDev.getName());
            }

            plan.addAction(annoAction);
            plan.addAction(asgAction);
        }

        _logger.trace("Exiting %s", fn);
    }

    /**
     * Checks the current Liqid and Kubernetes configuration to see if there is anything which would prevent
     * us from executing the requested command. Some problems can be warnings if _force is set.
     * In any case where processing cannot continue, we will throw an exception.
     * This is common code for the Initialize and Adopt commands.
     */
    protected boolean checkConfiguration(
        final Map<DeviceStatus, Node> computeDevices
    ) {
        var fn = "checkConfiguration";
        _logger.trace("Entering %s", fn);

        // Are there any compute device descriptions which contradict the node names?
        var errors = false;
        var errPrefix = getErrorPrefix();
        for (var entry : computeDevices.entrySet()) {
            var ds = entry.getKey();
            var node = entry.getValue();
            var di = _liqidInventory._deviceInfoById.get(entry.getKey().getDeviceId());
            var desc = di.getUserDescription();
            if (!(desc.equals("n/a") || desc.equals(node.getName()))) {
                System.err.printf("%s:Description for resource %s conflicts with node name %s",
                                  errPrefix,
                                  ds.getName(),
                                  node.getName());
                errors = true;
            }
        }

        var result = !errors;
        _logger.trace("Exiting %s with %s", fn, result);
        return result;
    }

    /**
     * Helpful wrapper to create a full annotation key
     */
    protected String createAnnotationKeyFor(
        final String keySuffix
    ) {
        return String.format("%s/%s", K8S_ANNOTATION_PREFIX, keySuffix);
    }

    /**
     * Helpful wrapper to create a full annotation key
     */
    protected String createAnnotationKeyForDeviceType(
        final LiqidGeneralType genType
    ) {
        return createAnnotationKeyFor(ANNOTATION_KEY_FOR_DEVICE_TYPE.get(genType));
    }

    /**
     * Creates a machine name for a combination of the compute device name and the k8s node name.
     */
    protected String createMachineName(
        final DeviceStatus devStat,
        final Node node
    ) {
        var devName = devStat.getName();
        var nodeName = node.getName();
        var machName = String.format("%s-%s", devName, nodeName);
        if (machName.length() > 22) {
            machName = machName.substring(0, 22);
        }

        return machName;
    }

    /**
     * Create machine, add compute device to machine, set compute device description to node name
     * machine name is limited to 22 characters, may not contain spaces, and must start with an alpha character.
     * We assume the compute devices have already been added to the targeted group.
     */
    protected void createMachines(
        final Plan plan,
        final Map<DeviceStatus, Node> computeDevices
    ) {
        // We're going to do it in order by pcpu{n} name, just because it is cleaner.
        var orderedMap = new TreeMap<Integer, DeviceStatus>();
        for (var ds : computeDevices.keySet()) {
            Integer key = Integer.parseInt(ds.getName().substring(4));
            orderedMap.put(key, ds);
        }

        for (var entry : orderedMap.entrySet()) {
            var ds = entry.getValue();
            var node = computeDevices.get(ds);
            var devName = ds.getName();
            var nodeName = node.getName();
            var machineName = createMachineName(ds, node);

            plan.addAction(new CreateMachine().setMachineName(machineName).setGroupName(_liqidGroupName));
            plan.addAction(new AssignToMachine().setMachineName(machineName).addDeviceName(devName));
            plan.addAction(new SetUserDescription().setDeviceName(devName).setDescription(nodeName));
            plan.addAction(new AnnotateNode().setNodeName(nodeName)
                                             .addAnnotation(Constants.K8S_ANNOTATION_MACHINE_NAME, machineName));
        }
    }

    /**
     * Creates a new Logger based on our current logger, which does NOT log to stdout or stderr.
     * Use for initializing lower-level libraries which you do not want to engage in the same verbosity
     * as the main code.
     */
    private Logger createSubLogger(
        final String name
    ) {
        var newLogger = new Logger(name, _logger);
        for (var w : newLogger.getWriters()) {
            if ((w instanceof StdOutWriter) || (w instanceof StdErrWriter)) {
                newLogger.removeWriter(w);
            }
        }
        return newLogger;
    }

    /**
     * Based on processor specifications, we populate containers of compute device information.
     * @param processorSpecs list of processor specifications which tie compute resources to k8s nodes
     *                       format is {deviceName} ':' {nodeName}
     * @param computeDevices map we create based on processorSpecs, tying deviceStatus to corresponding k8s node
     * @return true if successful, else false
     */
    protected boolean developComputeList(
        final Collection<String> processorSpecs,
        final Map<DeviceStatus, Node> computeDevices
    ) throws K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = "developDeviceList";
        _logger.trace("Entering %s with processorSpecs=%s", fn, processorSpecs);

        var errors = false;
        var errPrefix = getErrorPrefix();

        for (var spec : processorSpecs) {
            var split = spec.split(":");
            if (split.length != 2) {
                System.err.printf("ERROR:Invalid format for spec '%s'\n", spec);
                errors = true;
            }

            var devName = split[0];
            var nodeName = split[1];

            var devStat = _liqidInventory._deviceStatusByName.get(devName);
            if (devStat == null) {
                System.err.printf("%s:Compute resource '%s' is not in the Liqid Cluster\n", errPrefix, devName);
                errors = true;
            }

            Node node = null;
            try {
                node = _k8sClient.getNode(nodeName);
            } catch (K8SHTTPError ex) {
                if (ex.getResponseCode() == 404) {
                    System.err.printf("%s:Worker node '%s' is not in the Kubernetes Cluster\n", errPrefix, nodeName);
                    errors = true;
                } else {
                    throw ex;
                }
            }

            if ((devStat != null) && (node != null)) {
                computeDevices.put(devStat, node);
            }
        }

        var result = !errors;
        _logger.trace("Exiting %s with %s, computeDevices=%s", fn, errors, computeDevices);
        return result;
    }

    protected boolean developDeviceList(
        final Collection<String> resourceSpecs,
        final Collection<DeviceStatus> resourceDevices
    ) {
        var fn = "developDeviceList";
        _logger.trace("Entering %s with resourceSpecs=%s", fn, resourceSpecs);

        var errors = false;
        var errPrefix = getErrorPrefix();

        for (var spec : resourceSpecs) {
            var devStat = _liqidInventory._deviceStatusByName.get(spec);
            if (devStat == null) {
                System.err.printf("%s:Resource '%s' is not in the Liqid Cluster\n", errPrefix, spec);
                errors = true;
            } else {
                resourceDevices.add(devStat);
            }
        }

        var result = !errors;
        _logger.trace("Exiting %s with %s, resourceDevices=%s", fn, errors, resourceDevices);
        return result;
    }

    /**
     * Presuming _liqidInventory is populated, we find the compute device for a given machine.
     */
    protected DeviceStatus getComputeDeviceStatusForMachine(
        final Integer machineId
    ) {
        return _liqidInventory._deviceStatusByMachineId.get(machineId)
                                                       .stream()
                                                       .filter(ds -> ds.getDeviceType() == DeviceType.COMPUTE)
                                                       .findFirst()
                                                       .orElse(null);
    }

    protected String getErrorPrefix() {
        return _force ? "WARNING" : "ERROR";
    }

    /**
     * Given two sets of the same type, we populate a third set of that type with
     * only those items which are contained in both of the original sets.
     * @param set1 first contributing set
     * @param set2 second contributing set
     * @param intersection result set.
     * @param <T> item type
     */
    protected <T> void getIntersection(
        final Collection<T> set1,
        final Collection<T> set2,
        final LinkedList<T> intersection
    ) {
        intersection.clear();
        set1.stream().filter(set2::contains).forEach(intersection::add);
    }

    /**
     * Convenience method
     */
    protected void getLiqidInventory() throws LiqidException {
        _liqidInventory = LiqidInventory.getLiqidInventory(_liqidClient, _logger);
    }

    /**
     * This code solicits the information we need to interact with the Liqid Cluster from the k8s database.
     * It presumes that the k8s cluster is suitably linked to a Liqid Cluster.
     * Such linkage exists in the form of a ConfigMap and an optional Secret.
     * The specific bits of information returned include:
     *      IP address of the Liqid Cluster (actually, of the director)
     *      Group name of the Liqid Cluster group to which all relevant resources do, or should, belong.
     *      Username credential if basic authentication is enabled for the Liqid cluster
     *      Password credential if basic authentication is enabled for the Liqid cluster,
     *          although we do account for the possibility of a null password for a sadly unprotected username.
     * @throws ConfigurationDataException Indicates something is wrong in the actual bits of information stored in
     *                                      the configmap or the secret.
     * @throws K8SHTTPError If any unexpected HTTP responses are received. Generally, we expect only 200s.
     * @throws K8SJSONError If information received from k8s cannot be converted from JSON into the expected data
     *                          structs. This generally indicates a programming error on our part, but it could also
     *                          result from gratuitous changes in k8s, which does unfortunately occur.
     * @throws K8SRequestError Indicates some other error during processing, from within the k8sClient module.
     */
    protected void getLiqidLinkage(
    ) throws ConfigurationDataException, K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = "getLiqidLinkage";
        _logger.trace("Entering %s", fn);

        var cfgMap = _k8sClient.getConfigMap(K8S_CONFIG_NAMESPACE, K8S_CONFIG_NAME);
        _liqidAddress = cfgMap.data.get(K8S_CONFIG_MAP_IP_ADDRESS_KEY);
        _liqidGroupName = cfgMap.data.get(K8S_CONFIG_MAP_GROUP_NAME_KEY);

        _liqidUsername = null;
        _liqidPassword = null;
        try {
            var secret = _k8sClient.getSecret(K8S_SECRET_NAMESPACE, K8S_SECRET_NAME);
            var creds = new CredentialMangler(secret.data.get(K8S_SECRET_CREDENTIALS_KEY));
            _liqidUsername = creds.getUsername();
            _liqidPassword = creds.getPassword();
        } catch (K8SHTTPError kex) {
            // a 404 is okay - there might not be any credentials. Anything else gets rethrown.
            if (kex.getResponseCode() != 404) {
                throw kex;
            }
        }

        _logger.trace("Exiting %s", fn);
    }

    /**
     * Assuming _liqidInventory is populated *and* the given deviceStatus is a CPU device
     * *and* its user description has been populated with a corresponding K8s node name...
     * we return that node name.
     */
    protected String getK8sNodeNameFromComputeDevice(
        final DeviceStatus deviceStatus
    ) {
        return _liqidInventory._deviceInfoById.get(deviceStatus.getDeviceId()).getUserDescription();
    }

    protected boolean hasAnnotations() throws K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = "hasAnnotations";
        _logger.trace("Entering %s", fn);

        var result = false;
        var nodeEntities = _k8sClient.getNodes();
        outer: for (var node : nodeEntities) {
            var annos = node.metadata.annotations;
            for (var key : annos.keySet()) {
                if (key.startsWith(K8S_ANNOTATION_PREFIX)) {
                    result = true;
                    break outer;
                }
            }
        }

        _logger.trace("Exiting %s with %s", fn, result);
        return result;
    }

    /**
     * Checks to see whether linkage (either or both of configMap and secret) exists
     */
    protected boolean hasLinkage() throws K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = "hasLinkage";
        _logger.trace("Entering %s", fn);

        var result = false;
        try {
            var cfgMap = _k8sClient.getConfigMap(K8S_CONFIG_NAMESPACE, K8S_CONFIG_NAME);
            if (cfgMap != null) {
                result = true;
            }
        } catch (K8SHTTPError ex) {
            //  We *should* get here with a 404. Anything other than a 404 is a Bad Thing.
            if (ex.getResponseCode() != 404) {
                throw ex;
            }
        }

        if (!result) {
            try {
                var secret = _k8sClient.getSecret(K8S_SECRET_NAMESPACE, K8S_SECRET_NAME);
                if (secret != null) {
                    result = true;
                }
            } catch (K8SHTTPError ex) {
                //  We *should* get here with a 404. Anything other than a 404 is a Bad Thing.
                if (ex.getResponseCode() != 404) {
                    throw ex;
                }
            }
        }

        _logger.trace("Exiting %s with %s", fn, result);
        return result;
    }

    /**
     * Initializes a k8sClient object and stores the reference in our local attribute for the client.
     */
    protected void initK8sClient() throws K8SException {
        var fn = "initK8sClient";
        _logger.trace("Entering %s", fn);

        try {
            _k8sClient = new K8SClient(_proxyURL, createSubLogger(_logger.getName()));
        } catch (IOException ex) {
            _logger.catching(ex);
            var ex2 = new K8SException("Caught:" + ex.getMessage());
            _logger.throwing(ex2);
            throw ex2;
        }

        _logger.trace("Exiting %s", fn);
    }

    /**
     * Initializes a LiqidClient object and stores the reference in our local attribute for the client.
     * Address and credentials must be set ahead of time, either from command line parameters
     * or from linkage information.
     */
    protected void initLiqidClient() throws InternalErrorException, LiqidException {
        var fn = "initLiqidClient";
        _logger.trace("Entering %s", fn);

        try {
            _liqidClient = new LiqidClientBuilder().setHostAddress(_liqidAddress)
                                                   .setTimeoutInSeconds(_timeoutInSeconds)
                                                   .build();
        } catch (LiqidException ex) {
            _logger.catching(ex);
            var ex2 = new InternalErrorException("Caught:" + ex.getMessage());
            _logger.throwing(ex2);
            throw ex2;
        }

        _liqidClient.setLogger(createSubLogger("LiqidSDK"));
        if (_liqidUsername != null) {
            _liqidClient.login(LIQID_SDK_LABEL, _liqidUsername, _liqidPassword);
        }

        _logger.trace("Exiting %s", fn);
    }

    /**
     * Adds actions to the given plan to efficiently release all indicated devices from their containing groups.
     */
    protected void releaseDevicesFromGroups(
        final Plan plan,
        final Collection<DeviceStatus> devices
    ) {
        // Iterate over the groups so that we can do multiple devices per group.
        // In the case where we're removing all the devices for a group, just delete the group.
        for (var entry : _liqidInventory._deviceStatusByGroupId.entrySet()) {
            // If this group's name matches the liqid group name, we're going to delete it anyway.
            var groupId = entry.getKey();
            var group = _liqidInventory._groupsById.get(groupId);
            if (!group.getGroupName().equals(_liqidGroupName)) {
                var grpDevs = entry.getValue();
                var devsToRemove = new LinkedList<DeviceStatus>();
                getIntersection(devices, grpDevs, devsToRemove);

                if (!devsToRemove.isEmpty()) {
                    if (devsToRemove.size() == grpDevs.size()) {
                        plan.addAction(new DeleteGroup().setGroupName(group.getGroupName()));
                    } else {
                        var names = devsToRemove.stream()
                                                .map(DeviceStatus::getName)
                                                .collect(Collectors.toCollection(TreeSet::new));
                        plan.addAction(new RemoveFromGroup().setGroupName(group.getGroupName()).setDeviceNames(names));
                    }
                }
            }
        }
    }

    /**
     * Adds actions to the given plan to efficiently release all indicated devices from their containing machines.
     */
    protected void releaseDevicesFromMachines(
        final Plan plan,
        final Collection<DeviceStatus> devices
    ) {
        // Iterate over the machines so that we can do multiple devices per machine.
        // In the case where we're removing all the devices for a machine, just delete the machine.

        for (var entry : _liqidInventory._deviceStatusByMachineId.entrySet()) {
            // Find the group to which this machine belongs.
            // If that group's name matches _liqidGroupName, the whole group is going to be deleted,
            // and we don't need to do this mess for this machine.
            var machId = entry.getKey();
            var machine = _liqidInventory._machinesById.get(machId);
            var grpId = machine.getGroupId();
            var group = _liqidInventory._groupsById.get(grpId);
            if (!group.getGroupName().equals(_liqidGroupName)) {
                var machDevs = entry.getValue();
                var devsToRemove = new LinkedList<DeviceStatus>();
                getIntersection(devices, machDevs, devsToRemove);

                if (!devsToRemove.isEmpty()) {
                    if (devsToRemove.size() == machDevs.size()) {
                        plan.addAction(new DeleteMachine().setMachineName(machine.getMachineName()));
                    } else {
                        var devNames = devsToRemove.stream()
                                                   .map(DeviceStatus::getName)
                                                   .collect(Collectors.toCollection(TreeSet::new));
                        plan.addAction(new RemoveFromMachine().setMachineName(machine.getMachineName())
                                                              .setDeviceNames(devNames));
                    }
                }
            }
        }
    }
}
