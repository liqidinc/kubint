/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.annotate;

import com.bearsnake.komando.*;
import com.bearsnake.komando.exceptions.*;
import com.bearsnake.komando.values.*;
import com.liqid.k8s.Constants;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static com.liqid.k8s.annotate.Application.LOG_FILE_NAME;
import static com.liqid.k8s.annotate.CommandType.AUTO;
import static com.liqid.k8s.annotate.CommandType.LABEL;
import static com.liqid.k8s.annotate.CommandType.LINK;
import static com.liqid.k8s.annotate.CommandType.NODES;
import static com.liqid.k8s.annotate.CommandType.RESOURCES;
import static com.liqid.k8s.annotate.CommandType.UNLABEL;
import static com.liqid.k8s.annotate.CommandType.UNLINK;

public class Main {

    /*
    annotate auto
        -px,--proxy-url={proxy_url}
        [ -no,--no-update ]
        [ -f,--force ]

    annotate link
        -px,--proxy-url={proxy_url}
        -ip,--liqid-ip-address={ip_address}
        [ -u,--liqid-username={user_name} ]
        [ -p,--liqid-password={password} ]
        -g,--liqid-group={group_name}
        [ -f,--force ]

    annotate unlink
        -px,--proxy-url={proxy_url}
        [ -f,--force ]

    annotate label
        -px,--proxy-url={proxy_url}
        -n,--worker-node={worker_node_name}
        -m,--liqid-machine={liqid_machine}
        [ -fs,--fpga-spec={spec}[,...] ]
        [ -gs,--gpu-spec={spec}[,...] ]
        [ -ls,--link-spec={spec}[,...] ]
        [ -ms,--mem-spec={spec}[,...] ]
        [ -ss,--ssd-spec={spec}[,...] ]
        [ -f,--force ]

    annotate unlabel
        -px,--proxy-url={proxy_url}
        -n,--worker-node={worker_node_name}

    annotate nodes
        -px,--proxy-url={proxy_url}

    annotate resources
        -px,--proxy-url={proxy_url}
     */

    private static final CommandValue CV_AUTO = new CommandValue(AUTO.getToken());
    private static final CommandValue CV_LABEL = new CommandValue(CommandType.LABEL.getToken());
    private static final CommandValue CV_LINK = new CommandValue(LINK.getToken());
    private static final CommandValue CV_NODES = new CommandValue(CommandType.NODES.getToken());
    private static final CommandValue CV_RESOURCES = new CommandValue(CommandType.RESOURCES.getToken());
    private static final CommandValue CV_UNLABEL = new CommandValue(CommandType.UNLABEL.getToken());
    private static final CommandValue CV_UNLINK = new CommandValue(CommandType.UNLINK.getToken());

    private static final Switch ALL_SWITCH;
    private static final Switch FORCE_SWITCH;
    private static final Switch K8S_NODE_NAME_SWITCH;
    private static final Switch K8S_PROXY_URL_SWITCH;
    private static final Switch LIQID_ADDRESS_SWITCH;
    private static final Switch LIQID_GROUP_SWITCH;
    private static final Switch LIQID_MACHINE_SWITCH;
    private static final Switch LIQID_PASSWORD_SWITCH;
    private static final Switch LIQID_RESOURCE_FPGA_SWITCH;
    private static final Switch LIQID_RESOURCE_GPU_SWITCH;
    private static final Switch LIQID_RESOURCE_LINK_SWITCH;
    private static final Switch LIQID_RESOURCE_MEM_SWITCH;
    private static final Switch LIQID_RESOURCE_SSD_SWITCH;
    private static final Switch LIQID_USERNAME_SWITCH;
    private static final Switch LOGGING_SWITCH;
    private static final Switch NO_UPDATE_SWITCH;
    private static final Switch TIMEOUT_SWITCH;
    private static final CommandArgument COMMAND_ARG;

    static {
        try {
            K8S_NODE_NAME_SWITCH =
                new ArgumentSwitch.Builder().setShortName("n")
                                            .setLongName("worker-node")
                                            .setIsRequired(true)
                                            .addAffinity(CV_LABEL).addAffinity(CV_UNLABEL)
                                            .setValueName("worker_node_name")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies the node name of the Kubernetes node to be labeled or unlabeled.")
                                            .build();
            K8S_PROXY_URL_SWITCH =
                new ArgumentSwitch.Builder().setShortName("px")
                                            .setLongName("proxy-url")
                                            .setIsRequired(true)
                                            .setValueName("k8x_proxy_url")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies the URL for the kubectl proxy server.")
                                            .build();
            LIQID_ADDRESS_SWITCH =
                new ArgumentSwitch.Builder().setShortName("ip")
                                            .setLongName("liqid-ip-address")
                                            .setIsRequired(true)
                                            .addAffinity(CV_LINK)
                                            .setValueName("ip_address_or_dns_name")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies the URL for the director of the Liqid Cluster.")
                                            .build();
            LIQID_GROUP_SWITCH =
                new ArgumentSwitch.Builder().setShortName("g")
                                            .setLongName("liqid-group")
                                            .setIsRequired(true)
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
            LIQID_MACHINE_SWITCH =
                new ArgumentSwitch.Builder().setShortName("m")
                                            .setLongName("liqid-machine")
                                            .setIsRequired(true)
                                            .addAffinity(CV_LABEL)
                                            .setValueName("group_name")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies the Liqid Cluster machine name to be associated with a particular Kubernetes worker node.")
                                            .addDescription("The worker node corresponds to the particular Liqid CPU resource which hosts that node,")
                                            .addDescription("and that node is (or should be) assigned to the indicated machine.")
                                            .build();
            LIQID_PASSWORD_SWITCH =
                new ArgumentSwitch.Builder().setShortName("p")
                                            .setLongName("liqid-password")
                                            .setIsRequired(false)
                                            .addAffinity(CV_LINK)
                                            .setValueName("password")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies the password credential for the Liqid Directory.")
                                            .build();
            LIQID_RESOURCE_FPGA_SWITCH =
                new ArgumentSwitch.Builder().setShortName("fs")
                                            .setLongName("fpga-spec")
                                            .setIsRequired(false)
                                            .setIsMultiple(true)
                                            .addAffinity(CV_LABEL)
                                            .setValueName("specification")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies how many resources of this type should be assigned to the indicated worker node.")
                                            .addDescription("If there is no need to specify a particular vendor and model, the specification is simply an integer.")
                                            .addDescription("However, if the Liqid Cluster has multiple vendors or models of a particular resource type,")
                                            .addDescription("it is better to be specific regarding *which* models of that resource type are to be assigned")
                                            .addDescription("to the worker node. In this case, the specification format is:")
                                            .addDescription("  {vendor-name}:{model}:{count}")
                                            .addDescription("The vendor-name and model must exactly match what is reported by the " + RESOURCES.getToken() + " command.")
                                            .addDescription("If more than one specification is provided, the resources are additive. That is, one may enter")
                                            .addDescription("  -fs=acme:ft1000:2,acme:ft2000:1,5")
                                            .addDescription("which assigned 2 model ft1000 FPGA, 1 model ft2000 FPGA, and 5 other FPGAs of any model.")
                                            .addDescription("If not specified, no change is made to the relevant annotation.")
                                            .addDescription("To clear this value, enter '0' for the specification.")
                                            .build();
            LIQID_RESOURCE_GPU_SWITCH =
                new ArgumentSwitch.Builder().setShortName("gs")
                                            .setLongName("gpu-spec")
                                            .setIsRequired(false)
                                            .setIsMultiple(true)
                                            .addAffinity(CV_LABEL)
                                            .setValueName("specification")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies how many resources of this type should be assigned to the indicated worker node.")
                                            .addDescription("(see the documentation for the -fs,--fpga-spec switch.")
                                            .build();
            LIQID_RESOURCE_LINK_SWITCH =
                new ArgumentSwitch.Builder().setShortName("ls")
                                            .setLongName("link-spec")
                                            .setIsRequired(false)
                                            .setIsMultiple(true)
                                            .addAffinity(CV_LABEL)
                                            .setValueName("specification")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies how many resources of this type should be assigned to the indicated worker node.")
                                            .addDescription("(see the documentation for the -fs,--fpga-spec switch.")
                                            .build();
            LIQID_RESOURCE_MEM_SWITCH =
                new ArgumentSwitch.Builder().setShortName("ms")
                                            .setLongName("memory-spec")
                                            .setIsRequired(false)
                                            .setIsMultiple(true)
                                            .addAffinity(CV_LABEL)
                                            .setValueName("specification")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies how many resources of this type should be assigned to the indicated worker node.")
                                            .addDescription("(see the documentation for the -fs,--fpga-spec switch.")
                                            .build();
            LIQID_RESOURCE_SSD_SWITCH =
                new ArgumentSwitch.Builder().setShortName("ss")
                                            .setLongName("ssd-spec")
                                            .setIsRequired(false)
                                            .setIsMultiple(true)
                                            .addAffinity(CV_LABEL)
                                            .setValueName("specification")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies how many resources of this type should be assigned to the indicated worker node.")
                                            .addDescription("(see the documentation for the -fs,--fpga-spec switch.")
                                            .build();
            LIQID_USERNAME_SWITCH =
                new ArgumentSwitch.Builder().setShortName("u")
                                            .setLongName("liqid-username")
                                            .setIsRequired(false)
                                            .addAffinity(CV_LINK)
                                            .setValueName("username")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies the username credential for the Liqid Directory.")
                                            .build();
            LOGGING_SWITCH =
                new SimpleSwitch.Builder().setShortName("l")
                                          .setLongName("logging")
                                          .addDescription("Enables logging for error diagnostics.")
                                          .addDescription("Information will be written to " + LOG_FILE_NAME)
                                          .build();
            TIMEOUT_SWITCH =
                new ArgumentSwitch.Builder().setShortName("t")
                                            .setLongName("timeout")
                                            .setIsRequired(false)
                                            .setValueName("seconds")
                                            .setValueType(ValueType.FIXED_POINT)
                                            .addDescription("Timeout value for back-end network communication in seconds.")
                                            .build();
            ALL_SWITCH =
                new SimpleSwitch.Builder().setShortName("a")
                                          .setLongName("all")
                                          .addAffinity(CV_RESOURCES)
                                          .addDescription("Displays or processes all information, not just that which is normally processed.")
                                          .addDescription("See the related command(s) for more specific information.")
                                          .build();
            FORCE_SWITCH =
                new SimpleSwitch.Builder().setShortName("f")
                                          .setLongName("force")
                                          .addAffinity(CV_AUTO).addAffinity(CV_LINK).addAffinity(CV_UNLINK).addAffinity(CV_LABEL)
                                          .addDescription("Forces command to be executed in spite of certain (not all) detected problems.")
                                          .addDescription("In these cases, the detected problems are flagged as warnings rather than errors.")
                                          .build();
            NO_UPDATE_SWITCH =
                new SimpleSwitch.Builder().setShortName("no")
                                          .setLongName("no-update")
                                          .addAffinity(CV_AUTO).addAffinity(CV_LABEL)
                                          .addDescription("Indicates that no action should be taken; however, the script will display what action")
                                          .addDescription("/would/ be taken in the absence of this switch.")
                                          .build();
            COMMAND_ARG =
                new CommandArgument.Builder().addDescription(AUTO.getToken())
                                             .addDescription("  Automatically annotates Kubernetes worker nodes according to Kubernetes worker node names")
                                             .addDescription("  referenced by the user description fields of the various Liqid Cluster resources.")
                                             .addDescription("  Only resources which are attached to the linked Liqid Cluster group, or to machines within")
                                             .addDescription("  that group will be considered.")
                                             .addDescription("  Each worker node referenced by a compute resource will be annotated to refer to that resource.")
                                             .addDescription("  Then all the other resources will be allocated evenly (as possible) across the various")
                                             .addDescription("  worker nodes via subsequent annotations on those nodes.")
                                             .addDescription("  If any worker nodes are already annotated the process will not proceed unless -f,--force is specified,")
                                             .addDescription("  in which case all Liqid annotations will be cleared before proceeding.")
                                             .addDescription(LINK.getToken())
                                             .addDescription("  Links a particular Liqid Cluster to the targeted Kubernetes Cluster.")
                                             .addDescription("  Linking consists of storing certain Liqid Cluster information in the Kubernetes etcd database.")
                                             .addDescription("  Such information includes:")
                                             .addDescription("    An arbitrary name uniquely identifying the Liqid Cluster")
                                             .addDescription("    The IP address or DNS name for the Liqid Cluster Director")
                                             .addDescription("    Username credential for the Liqid Cluster Directory")
                                             .addDescription("    Password credential for the Liqid Cluster Directory")
                                             .addDescription("    Liqid Cluster resource group name identifying the resource group which is assigned to this Kubernetes cluster")
                                             .addDescription(UNLINK.getToken())
                                             .addDescription("  Unlinks a particular Liqid Cluster from the targeted Kubernetes Cluster.")
                                             .addDescription("  Removes the Liqid Cluster information provided via the " + LINK.getToken() + " command (listed above).")
                                             .addDescription("  Cannot be invoked so long as there are any existing node->machine labels.")
                                             .addDescription(LABEL.getToken())
                                             .addDescription("  Creates annotations for a particular Kubernetes worker node which identify, for that node, the following:")
                                             .addDescription("    The Liqid Cluster machine which is associated with the Kubernetes worker node")
                                             .addDescription("    The number (and optionally the model) of various resources to be assigned to the node")
                                             .addDescription(UNLABEL.getToken())
                                             .addDescription("  Removes annotations from a particular Kubernetes worker node, previously set by the " + LABEL.getToken() + " command (listed above).")
                                             .addDescription(NODES.getToken())
                                             .addDescription("  Displays existing Liqid-related configMap information and node annotations.")
                                             .addDescription(RESOURCES.getToken())
                                             .addDescription("  Displays Liqid Cluster resources associated with the linked Liqid Cluster group.")
                                             .addDescription("  If -a,-all is specified, all resources in the Liqid CLuster are displayed.")
                                             .addCommandValue(CV_AUTO)
                                             .addCommandValue(CV_LINK)
                                             .addCommandValue(CV_UNLINK)
                                             .addCommandValue(CV_LABEL)
                                             .addCommandValue(CV_UNLABEL)
                                             .addCommandValue(CV_NODES)
                                             .addCommandValue(CV_RESOURCES)
                                             .build();
        } catch (KomandoException e) {
            throw new RuntimeException(e);
        }
    }

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
     * Converts command line nonsense into configuration values which make sense.
     */
    private static boolean configureApplication(
        final Application application,
        final String[] args
    ) {
        try {
            CommandLineHandler clh = new CommandLineHandler();
            clh.addCanonicalHelpSwitch()
               .addCanonicalVersionSwitch()
               .addSwitch(K8S_NODE_NAME_SWITCH)
               .addSwitch(LIQID_ADDRESS_SWITCH)
               .addSwitch(LIQID_GROUP_SWITCH)
               .addSwitch(LIQID_MACHINE_SWITCH)
               .addSwitch(LIQID_USERNAME_SWITCH)
               .addSwitch(LIQID_PASSWORD_SWITCH)
               .addSwitch(LIQID_RESOURCE_FPGA_SWITCH)
               .addSwitch(LIQID_RESOURCE_GPU_SWITCH)
               .addSwitch(LIQID_RESOURCE_LINK_SWITCH)
               .addSwitch(LIQID_RESOURCE_MEM_SWITCH)
               .addSwitch(LIQID_RESOURCE_SSD_SWITCH)
               .addSwitch(LOGGING_SWITCH)
               .addSwitch(K8S_PROXY_URL_SWITCH)
               .addSwitch(ALL_SWITCH)
               .addSwitch(FORCE_SWITCH)
               .addSwitch(NO_UPDATE_SWITCH)
               .addSwitch(TIMEOUT_SWITCH)
               .addMutualExclusion(NO_UPDATE_SWITCH, FORCE_SWITCH)
               .addCommandArgument(COMMAND_ARG);

            var result = clh.processCommandLine(args);
            if (result.hasErrors() || result.hasWarnings()) {
                for (var msg : result._messages) {
                    System.err.println(msg);
                }
                System.err.println("Use --help for usage assistance");
                if (result.hasErrors()) {
                    return false;
                }
            } else if (result.isHelpRequested()) {
                clh.displayUsage("");
                return false;
            } else if (result.isVersionRequested()) {
                System.out.println("k8sIntegration Version " + Constants.VERSION);
                return false;
            }

            application.setLogging(result._switchSpecifications.containsKey(LOGGING_SWITCH))
                       .setCommandType(CommandType.get(result._commandValue.getValue()))
                       .setLiqidAddress(getSingleString(result._switchSpecifications.get(LIQID_ADDRESS_SWITCH)))
                       .setLiqidFPGASpecifications(getStringCollection(result._switchSpecifications.get(LIQID_RESOURCE_FPGA_SWITCH)))
                       .setLiqidGPUSpecifications(getStringCollection(result._switchSpecifications.get(LIQID_RESOURCE_GPU_SWITCH)))
                       .setLiqidGroupName(getSingleString(result._switchSpecifications.get(LIQID_GROUP_SWITCH)))
                       .setLiqidLinkSpecifications(getStringCollection(result._switchSpecifications.get(LIQID_RESOURCE_LINK_SWITCH)))
                       .setLiqidMachineName(getSingleString(result._switchSpecifications.get(LIQID_MACHINE_SWITCH)))
                       .setLiqidMemorySpecifications(getStringCollection(result._switchSpecifications.get(LIQID_RESOURCE_MEM_SWITCH)))
                       .setLiqidPassword(getSingleString(result._switchSpecifications.get(LIQID_PASSWORD_SWITCH)))
                       .setLiqidSSDSpecifications(getStringCollection(result._switchSpecifications.get(LIQID_RESOURCE_SSD_SWITCH)))
                       .setLiqidUsername(getSingleString(result._switchSpecifications.get(LIQID_USERNAME_SWITCH)))
                       .setK8SNodeName(getSingleString(result._switchSpecifications.get(K8S_NODE_NAME_SWITCH)))
                       .setProxyURL(getSingleString(result._switchSpecifications.get(K8S_PROXY_URL_SWITCH)))
                       .setAll(result._switchSpecifications.containsKey(ALL_SWITCH))
                       .setForce(result._switchSpecifications.containsKey(FORCE_SWITCH))
                       .setNoUpdate(result._switchSpecifications.containsKey(NO_UPDATE_SWITCH));

           var values = result._switchSpecifications.get(TIMEOUT_SWITCH);
            if ((values != null) && !values.isEmpty()) {
                application.setTimeoutInSeconds((int) (long) ((FixedPointValue) values.get(0)).getValue());
            }

            return true;
        } catch (KomandoException ex) {
            System.out.println("Internal error:" + ex.getMessage());
            return false;
        }
    }

    // ------------------------------------------------------------------------
    // program entry point
    // ------------------------------------------------------------------------

    public static void main(
        final String[] args
    ) {
        try {
            var app = new Application();
            if (configureApplication(app, args)) {
                app.process();
            }
        } catch (Throwable t) {
            // Last resort - hopefully we never hit this one
            System.err.println("An internal error occurred from which the application cannot recover.");
            System.err.println("Please capture this (and the following) information and report it to Liqid support.");
            System.err.println("Catching:" + t.getMessage());
            t.printStackTrace();
        }
    }
}
