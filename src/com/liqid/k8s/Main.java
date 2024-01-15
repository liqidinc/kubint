/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s;

import com.bearsnake.k8sclient.*;
import com.bearsnake.klog.*;
import com.bearsnake.komando.*;
import com.bearsnake.komando.exceptions.*;
import com.bearsnake.komando.values.*;
import com.liqid.k8s.commands.CommandType;
import com.liqid.k8s.exceptions.*;
import com.liqid.sdk.LiqidException;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.liqid.k8s.commands.CommandType.*;

/*
    adopt
        -px,--proxy-url={proxy_url}
        [ -pr,--processors={pcpu_name=worker_node_name}[,...] ]
        [ -r,--resources={name}[,...] ]
        [ -f,--force ]
        [ -no,--no-update ]

    annotate
        -px,--proxy-url={proxy_url}
        -a,--automatic
        -n,--worker-node={worker_node_name}
        [ -cl,--clear ]
        [ -m,--liqid-machine={liqid_machine} ]
        [ -fs,--fpga-spec={spec}[,...] ]
        [ -gs,--gpu-spec={spec}[,...] ]
        [ -ls,--link-spec={spec}[,...] ]
        [ -ms,--mem-spec={spec}[,...] ]
        [ -ss,--ssd-spec={spec}[,...] ]
        [ -f,--force ]
        [ -no,--no-update ]

    annotate -a
    annotate -n -cl
    annotate -n -m -fs -gs -ls -ms -ss

    compose
        -px,--proxy-url={proxy_url}
        [ -f,--force ]
        [ -no,--no-update ]

    initialize
        -px,--proxy-url={proxy_url}
        -ip,--liqid-ip-address={ip_address}
        [ -u,--liqid-username={user_name} ]
        [ -p,--liqid-password={password} ]
        -g,--liqid-group={group_name}
        -pr,--processors={pcpu_name:worker_node_name}[,...]
        -r,--resources={name}[,...]
        [ -al,--allocate ]
        [ -f,--force ]
        [ -no,--no-update ]

    link
        -px,--proxy-url={proxy_url}
        -ip,--liqid-ip-address={ip_address}
        [ -u,--liqid-username={user_name} ]
        [ -p,--liqid-password={password} ]
        -g,--liqid-group={group_name}
        [ -f,--force ]
        [ -no,--no-update ]

    nodes
        -px,--proxy-url={proxy_url}

    release
        -px,--proxy-url={proxy_url}
        -r,--resources={name}[,...]
        [ -f,--force ]
        [ -no,--no-update ]

    reset
        -px,--proxy-url={proxy_url}
        -ip,--liqid-ip-address={ip_address}
        [ -u,--liqid-username={user_name} ]
        [ -p,--liqid-password={password} ]
        [ -f,--force ]
        [ -no,--no-update ]

    resources
        -ip,--liqid-ip-address={ip_address}
        [ -u,--liqid-username={user_name} ]
        [ -p,--liqid-password={password} ]

    unlink
        -px,--proxy-url={proxy_url}
        [ -f,--force ]
        [ -no,--no-update ]
 */

public class Main {

    private static final String LOGGER_NAME = "Config";
    private static final String LOG_FILE_NAME = "liq-config.log";

    private static final CommandValue CV_ADOPT = new CommandValue(ADOPT.getToken());
    private static final CommandValue CV_ANNOTATE = new CommandValue(ANNOTATE.getToken());
    private static final CommandValue CV_COMPOSE = new CommandValue(COMPOSE.getToken());
    private static final CommandValue CV_INITIALIZE = new CommandValue(INITIALIZE.getToken());
    private static final CommandValue CV_LINK = new CommandValue(LINK.getToken());
    private static final CommandValue CV_NODES = new CommandValue(NODES.getToken());
    private static final CommandValue CV_RELEASE = new CommandValue(RELEASE.getToken());
    private static final CommandValue CV_RESET = new CommandValue(RESET.getToken());
    private static final CommandValue CV_RESOURCES = new CommandValue(RESOURCES.getToken());
    private static final CommandValue CV_UNLINK = new CommandValue(UNLINK.getToken());

    private static final CommandArgument COMMAND_ARG;
    private static final Switch ALLOCATE_SWITCH;
    private static final Switch AUTO_SWITCH;
    private static final Switch CLEAR_SWITCH;
    private static final Switch FORCE_SWITCH;
    private static final Switch FPGA_SPEC_SWITCH;
    private static final Switch GPU_SPEC_SWITCH;
    private static final Switch LIQID_ADDRESS_SWITCH;
    private static final Switch LIQID_GROUP_SWITCH;
    private static final Switch LIQID_PASSWORD_SWITCH;
    private static final Switch LIQID_USERNAME_SWITCH;
    private static final Switch LINK_SPEC_SWITCH;
    private static final Switch LOGGING_SWITCH;
    private static final Switch MACHINE_NAME_SWITCH;
    private static final Switch MEM_SPEC_SWITCH;
    private static final Switch NODE_NAME_SWITCH;
    private static final Switch NO_UPDATE_SWITCH;
    private static final Switch PROCESSORS_SWITCH;
    private static final Switch PROXY_URL_SWITCH;
    private static final Switch RESOURCES_SWITCH;
    private static final Switch SSD_SPEC_SWITCH;
    private static final Switch TIMEOUT_SWITCH;

    private static final Set<Switch> ANNOTATE_REQ_SET = new HashSet<>();

    static {
        try {
            ALLOCATE_SWITCH =
                new SimpleSwitch.Builder().setShortName("al")
                                          .setLongName("allocate")
                                          .addAffinity(CV_INITIALIZE)
                                          .addDescription("Causes the initialize process to create annotations and subsequently")
                                          .addDescription("allocate resources to worker nodes, as equally as possible per type.")
                                          .build();
            AUTO_SWITCH =
                new SimpleSwitch.Builder().setShortName("a")
                                          .setLongName("automatic")
                                          .addAffinity(CV_ANNOTATE)
                                          .addDescription("Automatically annotates all of the known workers in order to")
                                          .addDescription("evenly distribute (to the extent possible) the various resources")
                                          .addDescription("across the worker nodes. Does NOT recompose.")
                                          .build();
            CLEAR_SWITCH =
                new SimpleSwitch.Builder().setShortName("cl")
                                          .setLongName("clear")
                                          .addAffinity(CV_ANNOTATE)
                                          .addDescription("Clears all the resource annotations for the given node.")
                                          .build();
            FORCE_SWITCH =
                new SimpleSwitch.Builder().setShortName("f")
                                          .setLongName("force")
                                          .addAffinity(CV_ADOPT)
                                          .addAffinity(CV_ANNOTATE)
                                          .addAffinity(CV_COMPOSE)
                                          .addAffinity(CV_INITIALIZE)
                                          .addAffinity(CV_LINK)
                                          .addAffinity(CV_RELEASE)
                                          .addAffinity(CV_RESET)
                                          .addAffinity(CV_UNLINK)
                                          .addDescription("Forces command to be executed in spite of certain (not all) detected problems.")
                                          .addDescription("In these cases, the detected problems are flagged as warnings rather than errors.")
                                          .build();
            FPGA_SPEC_SWITCH =
                new ArgumentSwitch.Builder().setShortName("fs")
                                            .setLongName("fpga-spec")
                                            .setIsRequired(false)
                                            .setIsMultiple(true)
                                            .addAffinity(CV_ANNOTATE)
                                            .setValueName("specification")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies how many resources of this type should be assigned to the indicated worker node.")
                                            .addDescription("If there is no need to specify a particular vendor or model, the specification is simply an integer.")
                                            .addDescription("However, if the Liqid Cluster has multiple vendors or models of a particular resource type,")
                                            .addDescription("it is better to be specific regarding *which* models of that resource type are to be assigned")
                                            .addDescription("to the worker node. In this case, the specification format is:")
                                            .addDescription("  {vendor-name}:{model}:{count}")
                                            .addDescription("The vendor-name and model must exactly match what is reported by the " + RESOURCES.getToken() + " command.")
                                            .addDescription("If model is not specified, then the specification matches any model from the given vendor.")
                                            .addDescription("If more than one specification is provided, the resources are additive. That is, one may enter")
                                            .addDescription("  -fs=acme:ft1000:2,acme:ft2000:1,5")
                                            .addDescription("which assigns 2 model ft1000 FPGAs, 1 model ft2000 FPGA, and 5 other FPGAs of any model.")
                                            .addDescription("If not specified, no change is made to the relevant annotation.")
                                            .addDescription("To clear this value, enter '0' for the specification.")
                                            .build();
            GPU_SPEC_SWITCH =
                new ArgumentSwitch.Builder().setShortName("gs")
                                            .setLongName("gpu-spec")
                                            .setIsRequired(false)
                                            .setIsMultiple(true)
                                            .addAffinity(CV_ANNOTATE)
                                            .setValueName("specification")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies how many resources of this type should be assigned to the indicated worker node.")
                                            .addDescription("(see the documentation for the -fs,--fpga-spec switch.")
                                            .build();
            LIQID_ADDRESS_SWITCH =
                new ArgumentSwitch.Builder().setShortName("ip")
                                            .setLongName("liqid-ip-address")
                                            .setIsRequired(true)
                                            .addAffinity(CV_INITIALIZE)
                                            .addAffinity(CV_LINK)
                                            .addAffinity(CV_RESET)
                                            .addAffinity(CV_RESOURCES)
                                            .setValueName("ip_address_or_dns_name")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies the URL for the director of the Liqid Cluster.")
                                            .build();
            LIQID_GROUP_SWITCH =
                new ArgumentSwitch.Builder().setShortName("g")
                                            .setLongName("liqid-group")
                                            .setIsRequired(true)
                                            .addAffinity(CV_INITIALIZE)
                                            .addAffinity(CV_LINK)
                                            .setValueName("group_name")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies the Liqid Cluster group name to be associated with this Liqid Cluster.")
                                            .addDescription("Each worker node is associated with a CPU resource, and all other resources such as FPGAs and GPUs")
                                            .addDescription("are attached to this group's free pool, or to a machine within this group.")
                                            .addDescription("Note that there is a one-to-one correspondence between Liqid CPU resources and Liqid Machine definitions.")
                                            .addDescription("Specifically, any Liqid resource which is to be associated with the Kubernetes Cluster,")
                                            .addDescription("must be a part of this group.")
                                            .build();
            LIQID_PASSWORD_SWITCH =
                new ArgumentSwitch.Builder().setShortName("p")
                                            .setLongName("liqid-password")
                                            .setIsRequired(false)
                                            .addAffinity(CV_INITIALIZE)
                                            .addAffinity(CV_LINK)
                                            .addAffinity(CV_RESET)
                                            .addAffinity(CV_RESOURCES)
                                            .setValueName("password")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies the password credential for the Liqid Directory.")
                                            .build();
            LIQID_USERNAME_SWITCH =
                new ArgumentSwitch.Builder().setShortName("u")
                                            .setLongName("liqid-username")
                                            .setIsRequired(false)
                                            .addAffinity(CV_INITIALIZE)
                                            .addAffinity(CV_LINK)
                                            .addAffinity(CV_RESET)
                                            .addAffinity(CV_RESOURCES)
                                            .setValueName("username")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies the username credential for the Liqid Directory.")
                                            .build();
            LINK_SPEC_SWITCH =
                new ArgumentSwitch.Builder().setShortName("ls")
                                            .setLongName("link-spec")
                                            .setIsRequired(false)
                                            .setIsMultiple(true)
                                            .addAffinity(CV_ANNOTATE)
                                            .setValueName("specification")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies how many resources of this type should be assigned to the indicated worker node.")
                                            .addDescription("(see the documentation for the -fs,--fpga-spec switch.")
                                            .build();
            LOGGING_SWITCH =
                new SimpleSwitch.Builder().setShortName("l")
                                          .setLongName("logging")
                                          .addDescription("Enables logging for error diagnostics.")
                                          .addDescription("Information will be written to " + LOG_FILE_NAME)
                                          .build();
            MACHINE_NAME_SWITCH =
                new ArgumentSwitch.Builder().setShortName("m")
                                            .setLongName("machine-name")
                                            .setIsRequired(false)
                                            .addAffinity(CV_ANNOTATE)
                                            .setValueName("machine_name")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies the Liqid Cluster machine name to be associated with a particular Kubernetes worker node.")
                                            .addDescription("The worker node corresponds to the particular Liqid CPU resource which hosts that node,")
                                            .addDescription("and that node is (or should be) assigned to the indicated machine.")
                                            .build();
            MEM_SPEC_SWITCH =
                new ArgumentSwitch.Builder().setShortName("ms")
                                            .setLongName("memory-spec")
                                            .setIsRequired(false)
                                            .setIsMultiple(true)
                                            .addAffinity(CV_ANNOTATE)
                                            .setValueName("specification")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies how many resources of this type should be assigned to the indicated worker node.")
                                            .addDescription("(see the documentation for the -fs,--fpga-spec switch.")
                                            .build();
            NODE_NAME_SWITCH =
                new ArgumentSwitch.Builder().setShortName("n")
                                            .setLongName("node-name")
                                            .setIsRequired(false)
                                            .addAffinity(CV_ANNOTATE)
                                            .setValueName("node_name")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies the node name of the Kubernetes node to be labeled or unlabeled.")
                                            .build();
            NO_UPDATE_SWITCH =
                new SimpleSwitch.Builder().setShortName("no")
                                          .setLongName("no-update")
                                          .addAffinity(CV_ADOPT)
                                          .addAffinity(CV_ANNOTATE)
                                          .addAffinity(CV_COMPOSE)
                                          .addAffinity(CV_INITIALIZE)
                                          .addAffinity(CV_LINK)
                                          .addAffinity(CV_RELEASE)
                                          .addAffinity(CV_RESET)
                                          .addAffinity(CV_UNLINK)
                                          .addDescription("Indicates that no action should be taken; however, the script will display what action")
                                          .addDescription("/would/ be taken in the absence of this switch.")
                                          .build();
            PROCESSORS_SWITCH =
                new ArgumentSwitch.Builder().setShortName("pr")
                                            .setLongName("processors")
                                            .setValueType(ValueType.STRING)
                                            .setValueName("spec")
                                            .setIsRequired(false)
                                            .setIsMultiple(true)
                                            .addAffinity(CV_ADOPT)
                                            .addAffinity(CV_INITIALIZE)
                                            .addDescription("List of processor (compute) resources and the corresponding worker node names as known")
                                            .addDescription("to the Kubernetes Cluster.")
                                            .addDescription("{spec} format is:")
                                            .addDescription("  {pcpu_name} ':' {node_name}")
                                            .addDescription("example:")
                                            .addDescription("  -pr=pcpu0:worker1,pcpu1:worker2,pcpu2:worker3")
                                            .build();
            PROXY_URL_SWITCH =
                new ArgumentSwitch.Builder().setShortName("px")
                                            .setLongName("proxy-url")
                                            .setIsRequired(true)
                                            .addAffinity(CV_ADOPT)
                                            .addAffinity(CV_ANNOTATE)
                                            .addAffinity(CV_COMPOSE)
                                            .addAffinity(CV_INITIALIZE)
                                            .addAffinity(CV_LINK)
                                            .addAffinity(CV_NODES)
                                            .addAffinity(CV_RELEASE)
                                            .addAffinity(CV_RESET)
                                            .addAffinity(CV_UNLINK)
                                            .setValueName("k8x_proxy_url")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies the URL for the kubectl proxy server.")
                                            .build();
            RESOURCES_SWITCH =
                new ArgumentSwitch.Builder().setShortName("r")
                                            .setLongName("resources")
                                            .setValueType(ValueType.STRING)
                                            .setValueName("resource_name")
                                            .setIsRequired(false)
                                            .setIsMultiple(true)
                                            .addAffinity(CV_ADOPT)
                                            .addAffinity(CV_INITIALIZE)
                                            .addAffinity(CV_RELEASE)
                                            .addDescription("List of non-compute resources which are to be considered candidates for attaching to")
                                            .addDescription("the compute resources associated with the Kubernetes Cluster.")
                                            .addDescription("example:")
                                            .addDescription("  -r=gpu0,gpu1,gpu2,mem0,mem1,mem2")
                                            .addDescription("For the " + RESET.getToken() + " command, this list may also include processor resources.")
                                            .build();
            SSD_SPEC_SWITCH =
                new ArgumentSwitch.Builder().setShortName("ss")
                                            .setLongName("ssd-spec")
                                            .setIsRequired(false)
                                            .setIsMultiple(true)
                                            .addAffinity(CV_ANNOTATE)
                                            .setValueName("specification")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies how many resources of this type should be assigned to the indicated worker node.")
                                            .addDescription("(see the documentation for the -fs,--fpga-spec switch.")
                                            .build();
            TIMEOUT_SWITCH =
                new ArgumentSwitch.Builder().setShortName("t")
                                            .setLongName("timeout")
                                            .setIsRequired(false)
                                            .setValueName("seconds")
                                            .setValueType(ValueType.FIXED_POINT)
                                            .addDescription("Timeout value for back-end network communication in seconds.")
                                            .build();
            COMMAND_ARG =
                new CommandArgument.Builder().addDescription(ADOPT.getToken())
                                             .addDescription("  Adopts additional resources (compute or otherwise) into the targeted Kubernetes Cluster.")
                                             .addDescription("  Specified compute resources must already be known to the Kubernetes Cluster as worker nodes.")
                                             .addDescription("  Moves the listed compute nodes into the configured Liqid Cluster group, adding the appropriate ")
                                             .addDescription("  worker node names to the description fields, and moves the listed resources into the group.")
                                             .addDescription(ANNOTATE.getToken())
                                             .addDescription("  Creates annotations for a particular Kubernetes worker node which identify, for that node, the following:")
                                             .addDescription("    The Liqid Cluster machine which is associated with the Kubernetes worker node")
                                             .addDescription("    The number (and optionally the model) of various resources to be assigned to the node")
                                             .addDescription(INITIALIZE.getToken())
                                             .addDescription("  Configures the Liqid Cluster for use in a Kubernetes Cluster.")
                                             .addDescription("    Creates a resource group if it does not exist.")
                                             .addDescription("    Moves the listed compute nodes into the group, adding the worker node names to the description fields.")
                                             .addDescription("    Moves the listed resources into the group.")
                                             .addDescription("  The Liqid Cluster nodes referenced on the command line must already be configured and running")
                                             .addDescription("  as worker nodes, and should have no resources assigned to them.")
                                             .addDescription(LINK.getToken())
                                             .addDescription("  Links a particular Liqid Cluster to the targeted Kubernetes Cluster.")
                                             .addDescription("  Linking consists of storing certain Liqid Cluster information in the Kubernetes etcd database.")
                                             .addDescription("  Such information includes:")
                                             .addDescription("    An arbitrary name uniquely identifying the Liqid Cluster")
                                             .addDescription("    The IP address or DNS name for the Liqid Cluster Director")
                                             .addDescription("    Username credential for the Liqid Cluster Directory")
                                             .addDescription("    Password credential for the Liqid Cluster Directory")
                                             .addDescription("    Liqid Cluster resource group name identifying the resource group which is assigned to this Kubernetes cluster")
                                             .addDescription(NODES.getToken())
                                             .addDescription("  Displays existing Liqid-related configMap information and node annotations.")
                                             .addDescription(RELEASE.getToken())
                                             .addDescription("  Releases resources (including compute resources) from the Kubernetes Cluster,")
                                             .addDescription("  as well as removing them from the Liqid Cluster group (if they still exist there).")
                                             .addDescription(RESET.getToken())
                                             .addDescription("  Entirely resets the configuration of the Liqid Cluster by deleting all groups and machines.")
                                             .addDescription("  Removes all Liqid annotations and other configuration information from the Kubernetes Cluster.")
                                             .addDescription(RESOURCES.getToken())
                                             .addDescription("  Displays the resources and machines available on the Liqid Cluster.")
                                             .addDescription(UNLINK.getToken())
                                             .addDescription("  Unlinks a particular Liqid Cluster from the targeted Kubernetes Cluster.")
                                             .addDescription("  Removes the Liqid Cluster information provided via the " + LINK.getToken() + " command (listed above).")
                                             .addDescription("  Cannot be invoked so long as there are any existing node->machine labels.")
                                             .addCommandValue(CV_ADOPT)
                                             .addCommandValue(CV_ANNOTATE)
                                             .addCommandValue(CV_COMPOSE)
                                             .addCommandValue(CV_INITIALIZE)
                                             .addCommandValue(CV_LINK)
                                             .addCommandValue(CV_NODES)
                                             .addCommandValue(CV_RELEASE)
                                             .addCommandValue(CV_RESET)
                                             .addCommandValue(CV_RESOURCES)
                                             .addCommandValue(CV_UNLINK)
                                             .build();

            ANNOTATE_REQ_SET.add(AUTO_SWITCH);
            ANNOTATE_REQ_SET.add(NODE_NAME_SWITCH);
        } catch (KomandoException e) {
            throw new RuntimeException(e);
        }
    }

    private static Logger _logger = null;
    private static boolean _logging = false;

    // ------------------------------------------------------------------------
    // helper functions
    // ------------------------------------------------------------------------

    // Takes the *first* Value object which caller promises is a StringValue, and returns the extracted String.
    // If the value is not a StringValue, we return null.
    private static String getSingleString(
        final List<Value> valueList
    ) {
        String result = null;
        if ((valueList != null) && !valueList.isEmpty()) {
            result = ((StringValue)valueList.get(0)).getValue();
        }
        return result;
    }

    // Presuming we have a collection of StringValue entities (but presented as String entities)
    // we convert that to a collection of String values extracted from the StringValue entities.
    // Any value which is not a string value is ignored.
    private static Collection<String> getStringCollection(
        final List<Value> valueList
    ) {
        LinkedList<String> strings = null;
        if (valueList != null) {
            strings = valueList.stream()
                               .filter(v -> v instanceof StringValue)
                               .map(v -> (StringValue) v)
                               .map(StringValue::getValue)
                               .collect(Collectors.toCollection(LinkedList::new));
        }
        return strings;
    }

    /**
     * Creates an Application object and loads it with the stuff we pulled from the command line
     */
    private static Application configureApplication(
        final Result result
    ) {
        var app = new Application().setCommandType(CommandType.get(result._commandValue.getValue()))
                                   .setAllocate(result._switchSpecifications.containsKey(ALLOCATE_SWITCH))
                                   .setAutomatic(result._switchSpecifications.containsKey(AUTO_SWITCH))
                                   .setClear(result._switchSpecifications.containsKey(CLEAR_SWITCH))
                                   .setForce(result._switchSpecifications.containsKey(FORCE_SWITCH))
                                   .setFPGASpecs(getStringCollection(result._switchSpecifications.get(FPGA_SPEC_SWITCH)))
                                   .setGPUSpecs(getStringCollection(result._switchSpecifications.get(GPU_SPEC_SWITCH)))
                                   .setLiqidAddress(getSingleString(result._switchSpecifications.get(LIQID_ADDRESS_SWITCH)))
                                   .setLiqidGroupName(getSingleString(result._switchSpecifications.get(LIQID_GROUP_SWITCH)))
                                   .setLiqidPassword(getSingleString(result._switchSpecifications.get(LIQID_PASSWORD_SWITCH)))
                                   .setLiqidUsername(getSingleString(result._switchSpecifications.get(LIQID_USERNAME_SWITCH)))
                                   .setLinkSpecs(getStringCollection(result._switchSpecifications.get(LINK_SPEC_SWITCH)))
                                   .setLogger(_logger)
                                   .setMachineName(getSingleString(result._switchSpecifications.get(MACHINE_NAME_SWITCH)))
                                   .setMemorySpecs(getStringCollection(result._switchSpecifications.get(MEM_SPEC_SWITCH)))
                                   .setNodeName(getSingleString(result._switchSpecifications.get(NODE_NAME_SWITCH)))
                                   .setNoUpdate(result._switchSpecifications.containsKey(NO_UPDATE_SWITCH))
                                   .setProcessorSpecs(getStringCollection(result._switchSpecifications.get(PROCESSORS_SWITCH)))
                                   .setProxyURL(getSingleString(result._switchSpecifications.get(PROXY_URL_SWITCH)))
                                   .setResourceSpecs(getStringCollection(result._switchSpecifications.get(RESOURCES_SWITCH)))
                                   .setSSDSpecs(getStringCollection(result._switchSpecifications.get(SSD_SPEC_SWITCH)));

        var values = result._switchSpecifications.get(TIMEOUT_SWITCH);
        if ((values != null) && !values.isEmpty()) {
            app.setTimeoutInSeconds((int) (long) ((FixedPointValue) values.get(0)).getValue());
        }

        return app;
    }

    /**
     * Initializes logging based on the logging switch
     */
    private static void initLogging() throws InternalErrorException {
        try {
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
        } catch (IOException ex) {
            throw new InternalErrorException(ex.toString());
        }
    }

    /**
     * Converts command line nonsense into configuration values which make sense.
     * Since this bit determines logging levels, we cannot initialize logging until after we return.
     * @return reference to command line handler result if successful,
     *          null if we failed or if we were asked for version or help.
     */
    private static Result parseCommandLine(
        final String[] args
    ) {
        CommandLineHandler clh = new CommandLineHandler();
        clh.addCanonicalHelpSwitch()
           .addCanonicalVersionSwitch()
           .addSwitch(ALLOCATE_SWITCH)
           .addSwitch(AUTO_SWITCH)
           .addSwitch(CLEAR_SWITCH)
           .addSwitch(FORCE_SWITCH)
           .addSwitch(FPGA_SPEC_SWITCH)
           .addSwitch(GPU_SPEC_SWITCH)
           .addSwitch(LIQID_ADDRESS_SWITCH)
           .addSwitch(LIQID_GROUP_SWITCH)
           .addSwitch(LIQID_USERNAME_SWITCH)
           .addSwitch(LIQID_PASSWORD_SWITCH)
           .addSwitch(LINK_SPEC_SWITCH)
           .addSwitch(LOGGING_SWITCH)
           .addSwitch(MACHINE_NAME_SWITCH)
           .addSwitch(MEM_SPEC_SWITCH)
           .addSwitch(NODE_NAME_SWITCH)
           .addSwitch(NO_UPDATE_SWITCH)
           .addSwitch(PROCESSORS_SWITCH)
           .addSwitch(PROXY_URL_SWITCH)
           .addSwitch(RESOURCES_SWITCH)
           .addSwitch(SSD_SPEC_SWITCH)
           .addSwitch(TIMEOUT_SWITCH)
           .addRequirementSet(CV_ANNOTATE, ANNOTATE_REQ_SET)
           .addDependency(MACHINE_NAME_SWITCH, NODE_NAME_SWITCH)
           .addDependency(FPGA_SPEC_SWITCH, NODE_NAME_SWITCH)
           .addDependency(GPU_SPEC_SWITCH, NODE_NAME_SWITCH)
           .addDependency(LINK_SPEC_SWITCH, NODE_NAME_SWITCH)
           .addDependency(MEM_SPEC_SWITCH, NODE_NAME_SWITCH)
           .addDependency(SSD_SPEC_SWITCH, NODE_NAME_SWITCH)
           .addMutualExclusion(AUTO_SWITCH, CLEAR_SWITCH)
           .addMutualExclusion(AUTO_SWITCH, NODE_NAME_SWITCH)
           .addMutualExclusion(AUTO_SWITCH, MACHINE_NAME_SWITCH)
           .addMutualExclusion(AUTO_SWITCH, FPGA_SPEC_SWITCH)
           .addMutualExclusion(AUTO_SWITCH, GPU_SPEC_SWITCH)
           .addMutualExclusion(AUTO_SWITCH, LINK_SPEC_SWITCH)
           .addMutualExclusion(AUTO_SWITCH, MEM_SPEC_SWITCH)
           .addMutualExclusion(AUTO_SWITCH, SSD_SPEC_SWITCH)
           .addMutualExclusion(CLEAR_SWITCH, MACHINE_NAME_SWITCH)
           .addMutualExclusion(CLEAR_SWITCH, FPGA_SPEC_SWITCH)
           .addMutualExclusion(CLEAR_SWITCH, GPU_SPEC_SWITCH)
           .addMutualExclusion(CLEAR_SWITCH, LINK_SPEC_SWITCH)
           .addMutualExclusion(CLEAR_SWITCH, MEM_SPEC_SWITCH)
           .addMutualExclusion(CLEAR_SWITCH, SSD_SPEC_SWITCH)
           .addCommandArgument(COMMAND_ARG);

        var result = clh.processCommandLine(args);
        if (result.hasErrors() || result.hasWarnings()) {
            for (var msg : result._messages) {
                System.err.println(msg);
            }
            System.err.println("Use --help for usage assistance");
            return null;
        } else if (result.isHelpRequested()) {
            clh.displayUsage("");
            return null;
        } else if (result.isVersionRequested()) {
            System.out.println("k8sIntegration Version " + Constants.VERSION);
            return null;
        }

        return result;
    }

    // ------------------------------------------------------------------------
    // program entry point
    // ------------------------------------------------------------------------

    public static void main(
        final String[] args
    ) {
        var result = parseCommandLine(args);
        if (result != null) {
            _logging = result._switchSpecifications.containsKey(LOGGING_SWITCH);
            try {
                initLogging();
                configureApplication(result).process();
            } catch (ConfigurationDataException ex) {
                _logger.catching(ex);
                System.err.println("Configuration Data inconsistency(ies) prevent further processing.");
                System.err.println("Please collect logging information and contact Liqid Support.");
            } catch (ConfigurationException ex) {
                System.err.println(ex.getMessage());
                System.err.println("Configuration inconsistency(ies) prevent further processing.");
            } catch (InternalErrorException ex) {
                _logger.catching(ex);
                System.err.println("An internal error has been detected in the application.");
                System.err.println("Please collect logging information and contact Liqid Support.");
            } catch (K8SJSONError kex) {
                _logger.catching(kex);
                System.err.println("Something went wrong while parsing JSON data from the Kubernetes cluster.");
                System.err.println("Please collect logging information and contact Liqid Support.");
            } catch (K8SHTTPError kex) {
                _logger.catching(kex);
                var code = kex.getResponseCode();
                System.err.printf("Received unexpected %d HTTP response from the Kubernetes API server.\n", code);
                System.err.println("Please verify that you have provided the correct IP address and port information,");
                System.err.println("and that the API server (or proxy server) is up and running.");
            } catch (K8SRequestError kex) {
                _logger.catching(kex);
                System.err.println("Could not complete the request to the Kubernetes API server.");
                System.err.println("Error: " + kex.getMessage());
                System.err.println("Please verify that you have provided the correct IP address and port information,");
                System.err.println("and that the API server (or proxy server) is up and running.");
            } catch (K8SException kex) {
                _logger.catching(kex);
                System.err.println("Could not communicate with the Kubernetes API server.");
                System.err.println("Error: " + kex.getMessage());
                System.err.println("Please verify that you have provided the correct IP address and port information,");
                System.err.println("and that the API server (or proxy server) is up and running.");
            } catch (LiqidException lex) {
                _logger.catching(lex);
                System.err.println("Could not complete the request due to an error communicating with the Liqid Cluster.");
                System.err.println("Error: " + lex.getMessage());
                System.err.println("Please verify that you have provided the correct IP address and port information,");
                System.err.println("and that the API server (or proxy server) is up and running.");
            } catch (ProcessingException pex) {
                System.err.println("Previous errors prevent further processing.");
            } catch (Throwable t) {
                // just in case anything else gets through
                System.out.println("Caught " + t.getMessage());
                t.printStackTrace();
                System.err.println("An internal error has been detected in the application.");
                System.err.println("Please collect logging information and contact Liqid Support.");
            }
        }
    }
}
