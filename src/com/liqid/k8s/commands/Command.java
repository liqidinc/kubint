/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.commands;

import com.bearsnake.k8sclient.K8SClient;
import com.bearsnake.k8sclient.K8SException;
import com.bearsnake.k8sclient.K8SHTTPError;
import com.bearsnake.k8sclient.K8SJSONError;
import com.bearsnake.k8sclient.K8SRequestError;
import com.bearsnake.klog.Logger;
import com.bearsnake.klog.StdErrWriter;
import com.bearsnake.klog.StdOutWriter;
import com.liqid.k8s.CredentialMangler;
import com.liqid.k8s.LiqidGeneralType;
import com.liqid.k8s.LiqidInventory;
import com.liqid.k8s.exceptions.ConfigurationDataException;
import com.liqid.k8s.exceptions.ConfigurationException;
import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.exceptions.ProcessingException;
import com.liqid.k8s.plan.Plan;
import com.liqid.sdk.DeviceStatus;
import com.liqid.sdk.Group;
import com.liqid.sdk.LiqidClient;
import com.liqid.sdk.LiqidClientBuilder;
import com.liqid.sdk.LiqidException;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.liqid.k8s.Constants.K8S_ANNOTATION_FPGA_ENTRY;
import static com.liqid.k8s.Constants.K8S_ANNOTATION_GPU_ENTRY;
import static com.liqid.k8s.Constants.K8S_ANNOTATION_LINK_ENTRY;
import static com.liqid.k8s.Constants.K8S_ANNOTATION_MEMORY_ENTRY;
import static com.liqid.k8s.Constants.K8S_ANNOTATION_PREFIX;
import static com.liqid.k8s.Constants.K8S_ANNOTATION_SSD_ENTRY;
import static com.liqid.k8s.Constants.K8S_CONFIG_MAP_GROUP_NAME_KEY;
import static com.liqid.k8s.Constants.K8S_CONFIG_MAP_IP_ADDRESS_KEY;
import static com.liqid.k8s.Constants.K8S_CONFIG_NAME;
import static com.liqid.k8s.Constants.K8S_CONFIG_NAMESPACE;
import static com.liqid.k8s.Constants.K8S_SECRET_CREDENTIALS_KEY;
import static com.liqid.k8s.Constants.K8S_SECRET_NAME;
import static com.liqid.k8s.Constants.K8S_SECRET_NAMESPACE;
import static com.liqid.k8s.Constants.LIQID_SDK_LABEL;

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

//    /**
//     * This is invoked in situations where worker node annotations should not exist.
//     * We check all the worker nodes in the k8s cluster to see if any of them have any liqid-related annotations.
//     * If any annotations exist, we do the following:
//     *      If we are not forcing, we display the list of offending worker nodes and return false.
//     *          This is considered an error and the invoker should not proceed with processing.
//     *      If we are forcing, we note the existence of annotations on the nodes as warnings,
//     *          delete the annotations, then return true. The invoker may continue processing.
//     * @param command The command which was issued - this is merely for nice-formatting the error/warning messages.
//     * @return true if the invoker may continue, false if processing should stop.
//     * @throws K8SHTTPError If any unexpected HTTP responses are received. Generally, we expect only 200s.
//     * @throws K8SJSONError If information received from k8s cannot be converted from JSON into the expected data
//     *                          structs. This generally indicates a programming error on our part, but it could also
//     *                          result from gratuitous changes in k8s, which does unfortunately occur.
//     * @throws K8SRequestError Indicates some other error during processing, from within the k8sClient module.
//     */
//    protected boolean checkForExistingAnnotations(
//        final String command
//    ) throws K8SHTTPError, K8SJSONError, K8SRequestError {
//        var fn = "checkForExistingAnnotations";
//        _logger.trace("Entering %s command=%s", fn, command);
//
//        var result = true;
//        var nodeEntities = _k8sClient.getNodes();
//        for (var node : nodeEntities) {
//            var annos = node.metadata.annotations;
//            for (var key : annos.keySet()) {
//                if (key.startsWith(K8S_ANNOTATION_PREFIX)) {
//                    if (_force) {
//                        System.err.printf("WARNING:Deleting Liqid annotations from worker %s...\n", node.getName());
//                        removeAnnotationsFromNode(node);
//                    } else {
//                        System.err.printf("ERROR:Worker node %s has Liqid annotations\n", node.getName());
//                        result = false;
//                    }
//                }
//            }
//        }
//
//        _logger.trace("Exiting %s with %s", fn, result);
//        return result;
//    }

//    /**
//     * This is invoked in situations where linkage (configmaps and secrets) should not exist.
//     * If any linkage exists, we do the following:
//     *      If we are not forcing, we display the problem and return false.
//     *          This is considered an error and the invoker should not proceed with processing.
//     *      If we are forcing, we note the existence of the linkage and remove it, then return true.
//     *          The invoker may continue processing.
//     * @param command The command which was issued - this is merely for nice-formatting the error/warning messages.
//     * @return true if the invoker may continue, false if processing should stop.
//     * @throws K8SHTTPError If any unexpected HTTP responses are received. Generally, we expect only 200s.
//     * @throws K8SJSONError If information received from k8s cannot be converted from JSON into the expected data
//     *                          structs. This generally indicates a programming error on our part, but it could also
//     *                          result from gratuitous changes in k8s, which does unfortunately occur.
//     * @throws K8SRequestError Indicates some other error during processing, from within the k8sClient module.
//     */
//    protected boolean checkForExistingLinkage(
//        final String command
//    ) throws K8SHTTPError, K8SJSONError, K8SRequestError {
//        var fn = "checkForExistingLinkage";
//        _logger.trace("Entering %s with command=%s", fn, command);
//
//        ConfigMapPayload cfgMap = null;
//        try {
//            cfgMap = _k8sClient.getConfigMap(K8S_CONFIG_NAMESPACE, K8S_CONFIG_NAME);
//        } catch (K8SHTTPError ex) {
//            //  We *should* get here with a 404. Anything other than a 404 is a Bad Thing.
//            if (ex.getResponseCode() != 404) {
//                throw ex;
//            }
//        }
//
//        SecretPayload secret = null;
//        try {
//            secret = _k8sClient.getSecret(K8S_SECRET_NAMESPACE, K8S_SECRET_NAME);
//        } catch (K8SHTTPError ex) {
//            //  We *should* get here with a 404. Anything other than a 404 is a Bad Thing.
//            if (ex.getResponseCode() != 404) {
//                throw ex;
//            }
//        }
//
//        if ((cfgMap != null) || (secret != null)) {
//            if (!_force) {
//                System.err.println("ERROR:A link already exists between the Kubernetes Cluster and the Liqid Cluster.");
//                _logger.trace("Exiting %s false", fn);
//                return false;
//            }
//
//            System.err.println("WARNING:A link already exists between the Kubernetes Cluster and the Liqid Cluster.");
//        }
//
//        if (cfgMap != null) {
//            System.err.println("WARNING:Deleting config map entry...");
//            _k8sClient.deleteConfigMap(K8S_CONFIG_NAMESPACE, K8S_CONFIG_NAME);
//        }
//
//        if (secret != null) {
//            System.err.println("WARNING:Deleting secret entry...");
//            _k8sClient.deleteSecret(K8S_SECRET_NAMESPACE, K8S_SECRET_NAME);
//        }
//
//        _logger.trace("Exiting %s true", fn);
//        return true;
//    }

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
     * Displays devices based on the current known liqid inventory (see getLiqidInventory())
     * @param group Reference to Group if we want to limit the display to resources in that group, else null
     */
    protected void displayDevices(
        final Group group
    ) {
        var fn = "displayDevices";
        _logger.trace("Entering %s", fn);

        System.out.println();
        if (group == null) {
            System.out.println("All Resources:");
            System.out.println("  ---TYPE---  --NAME--  ----ID----  --------VENDOR--------  -----MODEL------  "
                                   + "-------MACHINE--------  -------------GROUP--------------  --DESCRIPTION--");
        } else {
            System.out.printf("Resources for group '%s':\n", group.getGroupName());
            System.out.println("  ---TYPE---  --NAME--  ----ID----  --------VENDOR--------  -----MODEL------  "
                                   + "-------MACHINE--------  --DESCRIPTION--");
        }

        for (var ds : _liqidInventory._deviceStatusByName.values()) {
            if (group != null) {
                if (!_liqidInventory._deviceStatusByGroupId.get(group.getGroupId()).contains(ds)) {
                    continue;
                }
            }

            var di = _liqidInventory._deviceInfoById.get(ds.getDeviceId());
            var str1 = String.format("%-10s  %-8s  0x%08x  %-22s  %-16s",
                                     ds.getDeviceType(),
                                     ds.getName(),
                                     ds.getDeviceId(),
                                     di.getVendor(),
                                     di.getModel());

            var dr = _liqidInventory._deviceRelationsByDeviceId.get(ds.getDeviceId());
            var machStr = "<none>";
            if (dr._machineId != null) {
                machStr = _liqidInventory._machinesById.get(dr._machineId).getMachineName();
            }

            var grpStr = "";
            if (group == null) {
                var temp = (dr._groupId == null)
                    ? "<none>"
                    : _liqidInventory._groupsById.get(dr._groupId).getGroupName();
                grpStr = String.format("  %-32s", temp);
            }

            System.out.printf("  %s  %-22s%s  %s\n", str1, machStr, grpStr, di.getUserDescription());
        }

        _logger.trace("Exiting %s", fn);
    }

    /**
     * Displays machines based on the current known liqid inventory (see getLiqidInventory())
     * @param group Reference to Group if we want to limit the display to machines in that group, else null
     */
    protected void displayMachines(
        final Group group
    ) {
        var fn = "displayMachines";
        _logger.trace("Entering %s", fn);

        System.out.println();
        if (group == null) {
            System.out.println("All Machines:");
            System.out.println("  -------------GROUP--------------  -------MACHINE--------  ----ID----  --------DEVICES---------");
        } else {
            System.out.printf("Machines for group '%s':\n", group.getGroupName());
            System.out.println("  -------MACHINE--------  ----ID----  --------DEVICES---------");
        }

        for (var mach : _liqidInventory._machinesById.values()) {
            var devNames = _liqidInventory._deviceStatusByMachineId.get(mach.getMachineId())
                                                   .stream()
                                                   .map(DeviceStatus::getName)
                                                   .collect(Collectors.toCollection(TreeSet::new));
            var devNamesStr = String.join(" ", devNames);

            if (group == null) {
                var grp = _liqidInventory._groupsById.get(mach.getGroupId());
                System.out.printf("  %-32s  %-22s  0x%08x  %s\n",
                                  grp.getGroupName(),
                                  mach.getMachineName(),
                                  mach.getMachineId(),
                                  devNamesStr);
            } else {
                if (mach.getGroupId().equals(group.getGroupId())) {
                    System.out.printf("  %-22s  0x%08x  %s\n",
                                      mach.getMachineName(),
                                      mach.getMachineId(),
                                      devNamesStr);
                }
            }
        }

        _logger.trace("Exiting %s", fn);
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

        var result = true;
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
}
