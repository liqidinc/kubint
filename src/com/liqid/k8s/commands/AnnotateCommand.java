/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.commands;

import com.bearsnake.k8sclient.*;
import com.bearsnake.klog.Logger;
import com.liqid.k8s.exceptions.*;
import com.liqid.k8s.layout.ClusterLayout;
import com.liqid.k8s.layout.GeneralType;
import com.liqid.k8s.layout.GenericResourceModel;
import com.liqid.k8s.layout.ResourceModel;
import com.liqid.k8s.layout.SpecificResourceModel;
import com.liqid.k8s.layout.VendorResourceModel;
import com.liqid.k8s.plan.Plan;
import com.liqid.k8s.plan.actions.AnnotateNodeAction;
import com.liqid.sdk.LiqidException;

import java.util.Collection;
import java.util.HashMap;

import static com.liqid.k8s.Constants.K8S_ANNOTATION_MACHINE_NAME;

public class AnnotateCommand extends Command {

    private Boolean _automatic;
    private Boolean _clear;
    private Collection<String> _fpgaSpecs;
    private Collection<String> _gpuSpecs;
    private Collection<String> _linkSpecs;
    private String _machineName;
    private Collection<String> _memorySpecs;
    private String _nodeName;
    private Collection<String> _ssdSpecs;

    public AnnotateCommand(
        final Logger logger,
        final Boolean force,
        final Integer timeoutInSeconds
    ) {
        super(logger, force, timeoutInSeconds);
    }

    public AnnotateCommand setAutomatic(final Boolean value) { _automatic = value; return this; }
    public AnnotateCommand setClear(final Boolean value) { _clear = value; return this; }
    public AnnotateCommand setFPGASpecifications(final Collection<String> list) { _fpgaSpecs = list; return this; }
    public AnnotateCommand setGPUSpecifications(final Collection<String> list) { _gpuSpecs = list; return this; }
    public AnnotateCommand setLinkSpecifications(final Collection<String> list) { _linkSpecs = list; return this; }
    public AnnotateCommand setMachineName(final String value) { _machineName = value; return this; }
    public AnnotateCommand setMemorySpecifications(final Collection<String> list) { _memorySpecs = list; return this; }
    public AnnotateCommand setNodeName(final String value) { _nodeName = value; return this; }
    public AnnotateCommand setProxyURL(final String value) { _proxyURL = value; return this; }
    public AnnotateCommand setSSDSpecifications(final Collection<String> list) { _ssdSpecs = list; return this; }

    /**
     * Creates plan actions to annotate the worker nodes.
     * We assume linkage already exists - so we can rely on the machine annotation in the k8s worker nodes
     * (if necessary) being correct, as well as the user descriptions in the compute devices in the Liqid Cluster
     * being correct also (again, if necessary).
     * ..
     * The plan does NOT move devices around - it only (re)annotates resource cluster counters.
     */
    private boolean processAutomatic(
        final Plan plan
    ) throws K8SHTTPError, K8SJSONError, K8SRequestError, ProcessingException, InternalErrorException {
        var fn = "processAutomatic";
        _logger.trace("Entering %s with plan=%s", fn, plan);

        var errors = false;

        var nodes = _k8sClient.getNodes();
        ClusterLayout layout = createEvenlyAllocatedClusterLayout(nodes);
        if (!createAnnotationsFromClusterLayout(nodes, layout, plan)) {
            errors = true;
        }

        var result = !errors;
        _logger.trace("%s returning %s", fn, result);
        return result;
    }

    /**
     * Creates plan actions to clear the resource annotations for a particular worker node.
     */
    private boolean processClear(
        final Plan plan
    ) throws K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = "processClear";
        _logger.trace("Entering %s with plan=%s", fn, plan);

        var errors = false;

        try {
            _k8sClient.getNode(_nodeName);
        } catch (K8SHTTPError kex) {
            if (kex.getResponseCode() == 404) {
                System.err.printf("ERROR:Node '%s' does not exist in the Kubernetes Cluster\n", _nodeName);
                errors = true;
            } else {
                throw kex;
            }
        }

        var action = new AnnotateNodeAction().setNodeName(_nodeName);
        for (var genType : GeneralType.values()) {
            if (genType != GeneralType.CPU) {
                var annoKey = ANNOTATION_KEY_FOR_DEVICE_TYPE.get(genType);
                action.addAnnotation(annoKey, null);
            }
        }
        plan.addAction(action);

        var result = !errors;
        _logger.trace("%s returning %s", fn, result);
        return result;
    }

    /**
     * Creates plan actions to annotate a particular worker node according to user-supplied specifications.
     * This *will* include a machine name annotation, so be prepared for that.
     */
    private boolean processManual(
        final Plan plan
    ) throws K8SHTTPError, K8SJSONError, K8SRequestError {
        var fn = "processManual";
        _logger.trace("Entering %s", fn);

        var errPrefix = getErrorPrefix();
        var errors = false;

        try {
            _k8sClient.getNode(_nodeName);
        } catch (K8SHTTPError kex) {
            if (kex.getResponseCode() == 404) {
                System.err.printf("ERROR:Node '%s' does not exist in the Kubernetes Cluster\n", _nodeName);
                errors = true;
            } else {
                _logger.throwing(kex);
                throw kex;
            }
        }

        var group = _liqidInventory.getGroup(_liqidGroupName);
        if (group == null) {
            System.err.printf("%s:Group '%s' does not exist in the Liqid Cluster\n", errPrefix, _liqidGroupName);
            if (!_force) {
                errors = true;
            }
        }

        if (_machineName != null) {
            var mach = _liqidInventory.getMachine(_machineName);
            if (mach == null) {
                System.err.printf("%s:Machine '%s' does not exist in the Liqid Cluster\n", errPrefix, _machineName);
                if (!_force) {
                    errors = true;
                }
            } else if ((group != null) && (!mach.getGroupId().equals(group.getGroupId()))) {
                System.err.printf("%s:Machine '%s' is not in group '%s'\n", errPrefix, _machineName, _liqidGroupName);
                if (!_force) {
                    errors = true;
                }
            }

            var annoKey = createAnnotationKeyFor(K8S_ANNOTATION_MACHINE_NAME);
            plan.addAction(new AnnotateNodeAction().setNodeName(_nodeName).addAnnotation(annoKey, _machineName));
        }

        if (_fpgaSpecs != null) {
            if (!processManualType(GeneralType.FPGA, _fpgaSpecs, plan)) {
                errors = true;
            }
        }

        if (_gpuSpecs != null) {
            if (!processManualType(GeneralType.GPU, _gpuSpecs, plan)) {
                errors = true;
            }
        }

        if (_linkSpecs != null) {
            if (!processManualType(GeneralType.LINK, _linkSpecs, plan)) {
                errors = true;
            }
        }

        if (_memorySpecs != null) {
            if (!processManualType(GeneralType.MEMORY, _memorySpecs, plan)) {
                errors = true;
            }
        }

        if (_ssdSpecs != null) {
            if (!processManualType(GeneralType.SSD, _ssdSpecs, plan)) {
                errors = true;
            }
        }

        var result = !errors;
        _logger.trace("%s returning %s", fn, result);
        return result;
    }

    private boolean processManualType(
        final GeneralType genType,
        final Collection<String> specifications,
        final Plan plan
    ) {
        var fn = "processType";
        _logger.trace("Entering %s genType=%s specifications=%s annotations=%s",
                      fn,
                      genType,
                      specifications);

        var errors = false;
        var errPrefix = getErrorPrefix();
        var clear = false;
        var resModelSpecs = new HashMap<ResourceModel, Integer>();

        for (var spec : specifications) {
            String vendor;
            String model;
            int resCount;

            var split = spec.split(":");
            if (split.length > 3) {
                System.err.printf("ERROR:Spec '%s' is invalid\n", spec);
                errors = true;
                continue;
            }

            try {
                resCount = Integer.parseInt(split[split.length - 1]);
                if (resCount < 0) {
                    throw new NumberFormatException("");
                }
            } catch (NumberFormatException ex) {
                System.err.printf("ERROR:Spec '%s' contains an invalid resource count\n", spec);
                errors = true;
                continue;
            }

            ResourceModel resModel;
            if (split.length == 3) {
                vendor = split[0];
                model = split[1];

                if (!_liqidInventory.hasDevice(vendor, model)) {
                    System.err.printf("%s:Spec '%s' refers to a vendor/model which is not present in the Liqid Cluster\n",
                                      errPrefix, spec);
                    if (!_force) {
                        errors = true;
                    }
                }

                resModel = new SpecificResourceModel(genType, vendor, model);
            } else if (split.length == 2) {
                vendor = split[0];

                if (!_liqidInventory.hasDevice(vendor)) {
                    System.err.printf("%s:Spec '%s' refers to a vendor which is not present in the Liqid Cluster\n",
                                      errPrefix, spec);
                    if (!_force) {
                        errors = true;
                    }
                }

                resModel = new VendorResourceModel(genType, vendor);
            } else {
                resModel = new GenericResourceModel(genType);
                if (resCount == 0) {
                    clear = true;
                }
            }

            if (resModelSpecs.containsKey(resModel)) {
                System.err.printf("%s:Spec '%s' overlays a previous specification\n", errPrefix, spec);
                errors = true;
            }

            resModelSpecs.put(resModel, resCount);
        }

        //  now that we have a map of resource models -> resource count, look for conflicts
        for (var entry : resModelSpecs.entrySet()) {
            var resModel1 = entry.getKey();
            var count1 = entry.getValue();
            if (count1 == 0) {
                for (var entry2 : resModelSpecs.entrySet()) {
                    if (!entry.equals(entry2)) {
                        var resModel2 = entry2.getKey();
                        var count2 = entry2.getValue();
                        if ((count2 > 0)
                            && resModel1.overlaps(resModel2)
                            && resModel2.isMoreSpecificThan(resModel1)) {
                            System.out.printf("%s:Conflict between specifications %s:%d and %s:%d\n",
                                              errPrefix, resModel1, count1, resModel2, count2);
                            errors = true;
                        }
                    }
                }
            }
        }

        if (errors && !_force) {
            _logger.trace("Exiting %s with false", fn);
            return false;
        }

        var parts = new String[resModelSpecs.size()];
        var px = 0;
        for (var entry : resModelSpecs.entrySet()) {
            var resModel = entry.getKey();
            var count = entry.getValue();
            if (resModel instanceof GenericResourceModel) {
                parts[px] = String.format("%d", count);
            } else if (resModel instanceof VendorResourceModel) {
                parts[px] = String.format("%s:%d", resModel.getVendorName(), count);
            } else if (resModel instanceof SpecificResourceModel) {
                parts[px] = String.format("%s:%s:%d", resModel.getVendorName(), resModel.getModelName(), count);
            }
            px++;
        }

        var annoKey = ANNOTATION_KEY_FOR_DEVICE_TYPE.get(genType);
        var annoValue = clear ? null : String.join(",", parts);
        plan.addAction(new AnnotateNodeAction().setNodeName(_nodeName).addAnnotation(annoKey, annoValue));

        _logger.trace("Exiting %s with true", fn);
        return true;
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

        var plan = new Plan();
        var success = false;
        if (_automatic) {
            success = processAutomatic(plan);
        } else if (_clear) {
            success = processClear(plan);
        } else {
            success = processManual(plan);
        }

        if (!success) {
            System.err.println("Errors prevent further processing.");
            _logger.trace("Exiting %s with null", fn);
            return null;
        }

        // All done
        _logger.trace("Exiting %s with %s", fn, plan);
        return plan;
    }
}
