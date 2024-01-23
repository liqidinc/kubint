/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.commands;

import com.bearsnake.k8sclient.*;
import com.bearsnake.klog.*;
import com.liqid.k8s.CredentialMangler;
import com.liqid.k8s.exceptions.*;
import com.liqid.k8s.layout.*;
import com.liqid.k8s.plan.Plan;
import com.liqid.k8s.plan.actions.*;
import com.liqid.sdk.*;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static com.liqid.k8s.Constants.*;

/**
 * Abstract class for all command handlers.
 * Most of the really useful functionality exists in this class, with the subclasses simply calling back
 * if and as necessary.
 */
public abstract class Command {

    protected static final Map<GeneralType, String> ANNOTATION_KEY_FOR_DEVICE_TYPE = new HashMap<>();
    static {
        ANNOTATION_KEY_FOR_DEVICE_TYPE.put(GeneralType.FPGA, K8S_ANNOTATION_FPGA_ENTRY);
        ANNOTATION_KEY_FOR_DEVICE_TYPE.put(GeneralType.GPU, K8S_ANNOTATION_GPU_ENTRY);
        ANNOTATION_KEY_FOR_DEVICE_TYPE.put(GeneralType.MEMORY, K8S_ANNOTATION_MEMORY_ENTRY);
        ANNOTATION_KEY_FOR_DEVICE_TYPE.put(GeneralType.LINK, K8S_ANNOTATION_LINK_ENTRY);
        ANNOTATION_KEY_FOR_DEVICE_TYPE.put(GeneralType.SSD, K8S_ANNOTATION_SSD_ENTRY);
    }

    protected final Logger _logger;
    protected Boolean _force;
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
     * Compares the annotations on the k8s worker nodes to the existing liqid configuration,
     * and creates an efficient plan for getting to the former from the latter.
     * @param plan the plan we populate
     */
    public void compose(
        final Plan plan
    ) throws K8SRequestError, K8SJSONError, K8SHTTPError {
        var fn = "compose";
        _logger.trace("Entering %s with plan=%s", fn, plan);

        for (var node : _k8sClient.getNodes()) {
            var annos = getLiqidAnnotations(node);
            if (!annos.isEmpty()) {

            }
        }

        _logger.trace("%s returning with plan=%s", plan);
    }

    /**
     * Helpful wrapper to create a full annotation key.
     * @param keySuffix the definitive portion of the key
     * @return the full key, with the company prefix applied to the front of the suffix
     */
    protected static String createAnnotationKeyFor(
        final String keySuffix
    ) {
        return String.format("%s/%s", K8S_ANNOTATION_PREFIX, keySuffix);
    }

    /**
     * Creates a string used as an annotation for a particular resource model and a count of resources
     * for that model.
     * @param resourceModel model of interest
     * @param count number of resources
     * @return string to be used in a specification annotation value
     */
    protected String createAnnotationForModelAndCount(
        final ResourceModel resourceModel,
        final Integer count
    ) throws InternalErrorException {
        if (resourceModel instanceof SpecificResourceModel srm) {
            return String.format("%s:%s:%d", resourceModel.getVendorName(), resourceModel.getModelName(), count);
        } else if (resourceModel instanceof VendorResourceModel vrm) {
            return String.format("%s:%d", resourceModel.getVendorName(), count);
        } else if (resourceModel instanceof GenericResourceModel grm) {
            return String.format("%d", count);
        } else {
            throw new InternalErrorException("Unrecognized resource model");
        }
    }

    /**
     * Creates actions to clear out all resource annotations from all known nodes,
     * then repopulate the annotations of the nodes based on the given ClusterLayout.
     * All the nodes referenced in the layout must be in the nodes collection, but the nodes collection
     * may contain nodes which are not in the layout.
     * We do expect each node's machine name annotation to be in place.
     * @param nodes collection of all the nodes of interest
     * @param layout ClusterLayout containing the desired resource layout
     * @param plan plan which will be populated with actions
     */
    protected void createAnnotationsFromClusterLayout(
        final Collection<Node> nodes,
        final ClusterLayout layout,
        final Plan plan
    ) throws InternalErrorException {
        var fn = "createAnnotationsFromClusterLayout";
        _logger.trace("Entering %s with nodes=%s layout=%s plan=%s", fn, nodes, layout, plan);

        // This is done by establishing removal annotations for all general types, for all nodes,
        // then overwriting the appopropriate removals with replacements per the cluster layout.
        for (var node : nodes) {
            var annotations = new HashMap<GeneralType, String>();
            for (var genType : GeneralType.values()) {
                if (genType != GeneralType.CPU) {
                    annotations.put(genType, null);
                }
            }

            var annoKey = createAnnotationKeyFor(K8S_ANNOTATION_MACHINE_NAME);
            var machineName = node.metadata.annotations.get(annoKey);
            var profile = layout.getMachineProfile(machineName);
            if (profile != null) {
                for (var resModel : profile.getResourceModels()) {
                    var genType = resModel.getGeneralType();
                    var count = profile.getCount(resModel);
                    var spec = createAnnotationForModelAndCount(resModel, count);
                    if (annotations.get(genType) != null) {
                        var sb = new StringBuilder();
                        sb.append(annotations.get(genType)).append(",").append(spec);
                        annotations.put(genType, sb.toString());
                    } else {
                        annotations.put(genType, spec);
                    }
                }
            }

            var action = new AnnotateNodeAction().setNodeName(node.getName());
            for (var entry : annotations.entrySet()) {
                var genType = entry.getKey();
                var spec = entry.getValue();
                action.addAnnotation(ANNOTATION_KEY_FOR_DEVICE_TYPE.get(genType), spec);
            }

            plan.addAction(action);
        }

        _logger.trace("%s returning with plan=%s", fn, plan);
    }

    /**
     * Creates a populates a ClusterLayout based on the Liqid annotations for the given worker nodes
     * @param nodes collection of worker nodes
     * @return populated ClusterLayout if successful, or null if errors exist and _force is not set
     */
    protected ClusterLayout createClusterLayoutFromAnnotations(
        final Collection<Node> nodes
    ) {
        var fn = "createClusterLayoutFromAnnotations";
        _logger.trace("Entering %s", fn);

        var errors = false;
        var errPrefix = getErrorPrefix();

        var layout = new ClusterLayout();
        for (var node : nodes) {
            var annoKey = createAnnotationKeyFor(K8S_ANNOTATION_MACHINE_NAME);
            var machineName = node.metadata.annotations.get(annoKey);
            if (machineName == null) {
                System.err.printf("%s:Node '%s' is not annotated with a valid machine name\n",
                                  errPrefix, node.getName());
                errors = true;
                continue;
            }

            var machProfile = new MachineProfile(machineName);
            for (var anno : getLiqidAnnotations(node).entrySet()) {
                var split = anno.getKey().split("/");
                GeneralType genType = null;
                if (split.length == 2) {
                    genType = switch (split[1]) {
                        case K8S_ANNOTATION_FPGA_ENTRY -> GeneralType.FPGA;
                        case K8S_ANNOTATION_GPU_ENTRY -> GeneralType.GPU;
                        case K8S_ANNOTATION_LINK_ENTRY -> GeneralType.LINK;
                        case K8S_ANNOTATION_MEMORY_ENTRY -> GeneralType.MEMORY;
                        case K8S_ANNOTATION_SSD_ENTRY -> GeneralType.SSD;
                        default -> null;
                    };
                }

                if (genType != null) {
                    var specs = anno.getValue().split(",");
                    for (var spec : specs) {
                        split = spec.split(":");
                        try {
                            ResourceModel resModel = null;
                            String vendor;
                            String model;
                            Integer count = null;
                            switch (split.length) {
                                case 1:
                                    count = Integer.parseInt(split[0]);
                                    resModel = new GenericResourceModel(genType);
                                    break;
                                case 2:
                                    vendor = split[0];
                                    count = Integer.parseInt(split[1]);
                                    resModel = new VendorResourceModel(genType, vendor);
                                    break;
                                case 3:
                                    vendor = split[0];
                                    model = split[1];
                                    count = Integer.parseInt(split[2]);
                                    resModel = new SpecificResourceModel(genType, vendor, model);
                                    break;
                                default:
                                    System.out.printf("%s:Annotation for node '%s' -> %s is invalid\n",
                                                      errPrefix, node.getName(), anno.getValue());
                                    errors = true;
                            }
                            machProfile.injectCount(resModel, count);
                        } catch (NumberFormatException ex) {
                            System.err.printf("%s:Annotation for node '%s' -> %s contains invalid resource count\n",
                                              errPrefix, node.getName(), anno.getValue());
                            errors = true;
                        }
                    }
                }
            }

            if (!machProfile.getResourceModels().isEmpty()) {
                layout.addMachineProfile(machProfile);
            }
        }

        var result = errors ? null : layout;
        _logger.trace("%s returning %s", fn, result);
        return result;
    }

    /**
     * Creates steps to create annotations which will allocate resources as equally as possible
     * among the k8s worker nodes. Does NOT require anything from the existing _liqidClient nor from _k8sClient.
     * This logic is here because we really need to tie nodes to machines (sometimes in the context of NOT
     * having the annotation for the node->machine name in place), and we need to know how many devices of each
     * generic type we have, which we could get many places... but all of that in one place is best provided
     * in the .commands package, which is where we are at.
     * @param computeDeviceItems container which indicates the compute devices of interest, along with their nodes
     * @param resourceDeviceItems container which indicates the resource devices of interest
     * @return ClusterLayout object corresponding to the actions added to the given Plan
     */
    protected ClusterLayout createEvenlyAllocatedClusterLayout(
        final Map<DeviceItem, Node> computeDeviceItems,
        final Collection<DeviceItem> resourceDeviceItems
    ) {
        var fn = "allocateEqually";
        _logger.trace("Entering %s with compDevs=%s resDevs=%s",
                      fn, computeDeviceItems, resourceDeviceItems);

        var layout = new ClusterLayout();
        var devsByType = LiqidInventory.segregateDeviceItemsByType(resourceDeviceItems);
        if (computeDeviceItems.isEmpty() || resourceDeviceItems.isEmpty()) {
            _logger.trace("Exiting %s with nothing to do", fn);
            return layout;
        }

        //  We do this by machine, because it is much more efficient to attach all the devices for the machine
        //  in one step. We need to iterate over the computeDeviceItems to get there, though.
        var remainingMachineCount = computeDeviceItems.size();
        for (var entry : computeDeviceItems.entrySet()) {
            var node = entry.getValue();
            var machineName = createMachineName(entry.getKey().getDeviceStatus(), node);
            var machineProfile = new MachineProfile(machineName);
            var annoAction = new AnnotateNodeAction().setNodeName(node.getName());

            //  loop over each type of device - for each type we have all the still-to-be-assigned
            //  devices of that type.
            for (var subEntry : devsByType.entrySet()) {
                var genType = subEntry.getKey();
                var devItems = subEntry.getValue();

                //  how many for this machine?
                var resCount = devItems.size() / remainingMachineCount;
                if (devItems.size() % remainingMachineCount > 0) {
                    resCount++;
                }

                //  Create resource profile and add it to machine profile with the resource count
                var resModel = new GenericResourceModel(genType);
                machineProfile.injectCount(resModel, resCount);

                for (int rx = 0; rx < resCount; rx++) {
                    devItems.removeFirst();
                }
            }

            remainingMachineCount--;

            if (!machineProfile.getResourceModels().isEmpty()) {
                layout.addMachineProfile(machineProfile);
            }
        }

        _logger.trace("%s returning %s", fn, layout);
        return layout;
    }

    /**
     * Creates a machine name for a combination of the compute device name and the k8s node name.
     * This is somewhat arbitrary, but it should be used whenever a machine name needs to be associated with
     * a particular compute device and worker node combination.
     * We ensure (to a reasonable extend) that the generated name is valid for a Liqid Cluster Machine.
     * @param devStat SDK DeviceStatus object representing a compute device
     * @param node Node object representing the k8w worker node corresponding to the compute device
     * @return name to be used for the Liqid Cluster machine.
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
     * Add actions to a plan which effect the creation of Liqid Cluster machines.
     * These actions create the machine, add the appropriate compute device to the machine,
     * and set the compute device user description to the appropriate k8s worker node name.
     * We assume the compute devices have already been added to the targeted group.
     * One side-effect which might be useful to the caller, is that we do the following link-up in the
     * computeDevices parameter:
     *  Each DeviceItem will be updated so that the user description field contains the corresponding node name
     *  Each Node item will be updated to contain an annotation tying the node to the corresponding Liqid machine
     * These changes are not persisted, but they may be used by the caller for any subsequent necessary processing.
     * @param computeDevices a map of DeviceItem objects representing compute resources, to the corresponding
     *                       Node objects representing k8s worker nodes.
     * @param plan the plan which we populate
     */
    protected void createMachines(
        final Map<DeviceItem, Node> computeDevices,
        final Plan plan
    ) {
        // We're going to do it in order by pcpu{n} name, just because it is cleaner.
        var orderedMap = new TreeMap<Integer, DeviceItem>();
        for (var devItem : computeDevices.keySet()) {
            Integer key = Integer.parseInt(devItem.getDeviceName().substring(4));
            orderedMap.put(key, devItem);
        }

        for (var entry : orderedMap.entrySet()) {
            var devItem = entry.getValue();
            var node = computeDevices.get(devItem);
            var devName = devItem.getDeviceName();
            var nodeName = node.getName();
            var machineName = createMachineName(devItem.getDeviceStatus(), node);

            plan.addAction(new CreateMachineAction().setMachineName(machineName).setGroupName(_liqidGroupName));
            plan.addAction(new AssignToMachineAction().setMachineName(machineName).addDeviceName(devName));
            plan.addAction(new SetUserDescriptionAction().setDeviceName(devName).setDescription(nodeName));
            plan.addAction(new AnnotateNodeAction().setNodeName(nodeName)
                                                   .addAnnotation(K8S_ANNOTATION_MACHINE_NAME, machineName));

            devItem.getDeviceInfo().setUserDescription(nodeName);
            if (computeDevices.get(devItem).metadata.annotations == null) {
                computeDevices.get(devItem).metadata.annotations = new HashMap<>();
            }
            computeDevices.get(devItem).metadata.annotations.put(createAnnotationKeyFor(K8S_ANNOTATION_MACHINE_NAME),
                                                                 machineName);
        }
    }

    /**
     * Creates a new Logger based on our current logger, which does NOT log to stdout or stderr.
     * Use for initializing lower-level libraries which you do not want to engage in the same verbosity
     * as the main code.
     * @param name name for the new logger
     * @return new Logger object
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
     * @param computeDevices a map of DeviceItem objects which we create based on the processorSpecs praameter,
     *                       which represents compute resources with their corresponding k8s Node objects.
     * @return true if successful, else false
     */
    protected boolean developComputeListFromSpecifications(
        final Collection<String> processorSpecs,
        final Map<DeviceItem, Node> computeDevices
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

            var devItem = _liqidInventory.getDeviceItem(devName);
            if (devItem == null) {
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

            if ((devItem != null) && (node != null)) {
                computeDevices.put(devItem, node);
            }
        }

        var result = !errors;
        _logger.trace("Exiting %s with %s, computeDevices=%s", fn, errors, computeDevices);
        return result;
    }

    /**
     * Based on resource specifications (i.e., device names) we develop a collection of corresponding
     * DeviceItem objects based on the provided LiqidInventory object
     * @param resourceSpecs list of resource (device) names - does NOT include compute resources
     * @param resourceDevices collection which we populate with DeviceItem objects
     * @return true if successful, else false
     */
    protected boolean developDeviceListFromSpecifications(
        final Collection<String> resourceSpecs,
        final Collection<DeviceItem> resourceDevices
    ) {
        var fn = "developDeviceList";
        _logger.trace("Entering %s with resourceSpecs=%s", fn, resourceSpecs);

        var errors = false;
        var errPrefix = getErrorPrefix();

        for (var spec : resourceSpecs) {
            var devItem = _liqidInventory.getDeviceItem(spec);
            if (devItem == null) {
                System.err.printf("%s:Resource '%s' is not in the Liqid Cluster\n", errPrefix, spec);
                errors = true;
            } else {
                resourceDevices.add(devItem);
            }
        }

        var result = !errors;
        _logger.trace("Exiting %s with %s, resourceDevices=%s", fn, errors, resourceDevices);
        return result;
    }

    /**
     * Retrieves a short string to be used in error/warning messages.
     * Anomalies are often warnings if -force is set, but errors otherwise
     * @return message prefix
     */
    protected String getErrorPrefix() {
        return _force ? "WARNING" : "ERROR";
    }

    /**
     * Given two collections of the same type, we populate a third collection of that type with
     * only those items which are contained in both of the original collections.
     * The basic type should have a meaningful equals operation.
     * For unordered collections, the resulting order will be that of collection1.
     * @param collection1 first contributing collection
     * @param collection2 second contributing collection
     * @param intersection resulting collection.
     * @param <T> item type
     */
    protected <T> void getIntersection(
        final Collection<T> collection1,
        final Collection<T> collection2,
        final Collection<T> intersection
    ) {
        intersection.clear();
        var temp2 = new LinkedList<>(collection2);
        for (var item : collection1) {
            if (temp2.contains(item)) {
                temp2.remove(item);
                intersection.add(item);
            }
        }
    }

    /**
     * Retrieves all the liqid annotations for a particular node from the k8s cluster
     * @return map containing liqid annotations
     */
    protected Map<String, String> getLiqidAnnotations(
        final Node node
    ) {
        var fn = "getLiqidAnnotations";
        _logger.trace("Entering %s with node=%s", fn, node);

        var result = new HashMap<String, String>();
        for (var anno : node.metadata.annotations.entrySet()) {
            if (anno.getKey().startsWith(K8S_ANNOTATION_PREFIX)) {
                result.put(anno.getKey(), anno.getValue());
            }
        }

        _logger.trace("%s returning %s", result);
        return result;
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

    protected boolean hasAnnotations() throws K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = "hasAnnotations";
        _logger.trace("Entering %s", fn);

        var nodeEntities = _k8sClient.getNodes();
        for (var node : nodeEntities) {
            var annos = node.metadata.annotations;
            for (var key : annos.keySet()) {
                if (key.startsWith(K8S_ANNOTATION_PREFIX)) {
                    _logger.trace("Exiting %s with true", fn);
                    return true;
                }
            }
        }

        _logger.trace("Exiting %s with false", fn);
        return false;
    }

    /**
     * Checks to see whether linkage (either or both of configMap and secret) exists
     * @return true if linkage exists, else false
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

        _liqidInventory = LiqidInventory.createLiqidInventory(_liqidClient);
        _logger.trace("Exiting %s", fn);
    }

    /**
     * Adds actions to the given plan to efficiently release all indicated devices from their containing groups.
     * Does NOT actually do anything else.
     * @param devices collection of DeviceItem objects to be removed from their containing groups
     * @param plan plan which we populate
     */
    protected void releaseDevicesFromGroups(
        final Collection<DeviceItem> devices,
        final Plan plan
    ) {
        var fn = "releaseDevicesFromGroups";
        _logger.trace("Entering %s with devices=%s", fn, devices);

        // Iterate over the groups so that we can do multiple devices per group.
        // In the case where we're removing all the devices for a group, just delete the group.
        for (var group : _liqidInventory.getGroups()) {
                var grpDevs = _liqidInventory.getDeviceItemsForGroup(group.getGroupId());
                Set<DeviceItem> devsToRemove = new HashSet<>();
                getIntersection(devices, grpDevs, devsToRemove);

                if (!devsToRemove.isEmpty()) {
                    if (devsToRemove.size() == grpDevs.size()) {
                        plan.addAction(new DeleteGroupAction().setGroupName(group.getGroupName()));
                    } else {
                        var names = LiqidInventory.getDeviceNamesFromItems(devsToRemove);
                        plan.addAction(new RemoveFromGroupAction().setGroupName(group.getGroupName())
                                                                  .setDeviceNames(names));
                    }
                }
        }

        _logger.trace("Exiting %s with plan=%s", fn, plan);
    }

    /**
     * Adds actions to the given plan to efficiently release all indicated devices from their containing machines.
     * Does NOT actually do anything else.
     * @param devices collection of DeviceItem objects to be removed from their containing machines
     * @param plan plan which we populate
     */
    protected void releaseDevicesFromMachines(
        final Collection<DeviceItem> devices,
        final Plan plan
    ) {
        var fn = "releaseDevicesFromMachines";
        _logger.trace("Entering %s with devices=%s", fn, devices);

        // Iterate over the machines so that we can do multiple devices per machine.
        // In the case where we're removing all the devices for a machine, just delete the machine.
        for (var mach : _liqidInventory.getMachines()) {
            var machDevs = _liqidInventory.getDeviceItemsForMachine(mach.getMachineId());
            Set<DeviceItem> devsToRemove = new HashSet<>();
            getIntersection(devices, machDevs, devsToRemove);

            if (!devsToRemove.isEmpty()) {
                if (devsToRemove.size() == machDevs.size()) {
                    plan.addAction(new DeleteMachineAction().setMachineName(mach.getMachineName()));
                } else {
                    var names = LiqidInventory.getDeviceNamesFromItems(devsToRemove);
                    plan.addAction(new RemoveFromMachineAction().setMachineName(mach.getMachineName())
                                                                .setDeviceNames(names));
                }
            }
        }

        _logger.trace("Exiting %s with plan=%s", fn, plan);
    }
}
