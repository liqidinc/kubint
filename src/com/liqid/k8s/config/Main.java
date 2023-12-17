/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.config;

import com.bearsnake.komando.*;
import com.bearsnake.komando.exceptions.*;
import com.bearsnake.komando.values.CommandValue;
import com.bearsnake.komando.values.FixedPointValue;
import com.bearsnake.komando.values.StringValue;
import com.bearsnake.komando.values.Value;
import com.bearsnake.komando.values.ValueType;
import com.liqid.k8s.Constants;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static com.liqid.k8s.config.CommandType.CLEANUP;
import static com.liqid.k8s.config.CommandType.EXECUTE;
import static com.liqid.k8s.config.CommandType.INITIALIZE;
import static com.liqid.k8s.config.CommandType.PLAN;
import static com.liqid.k8s.config.CommandType.RESET;
import static com.liqid.k8s.config.CommandType.RESOURCES;
import static com.liqid.k8s.config.CommandType.VALIDATE;

public class Main {

    /*
    config cleanup
        -px,--proxy-url={proxy_url}

    config execute
        -px,--proxy-url={proxy_url}

    config initialize
        -px,--proxy-url={proxy_url}
        -ip,--liqid-ip-address={ip_address}
        [ -u,--liqid-username={user_name} ]
        [ -p,--liqid-password={password} ]
        -g,--liqid-group={group_name}
        -pr,--processors={pcpu_name=worker_node_name}[,...]
        -r,--resources={name}[,...]
        [ -f,--force ]

    config plan
        -px,--proxy-url={proxy_url}

    config reset
        -px,--proxy-url={proxy_url}
        -ip,--liqid-ip-address={ip_address}
        [ -u,--liqid-username={user_name} ]
        [ -p,--liqid-password={password} ]
        -f,--force

    config resources
        -ip,--liqid-ip-address={ip_address}
        [ -u,--liqid-username={user_name} ]
        [ -p,--liqid-password={password} ]
        [ -f,--force ]

    config validate
        -px,--proxy-url={proxy_url}
    */

    private static final CommandValue CV_CLEANUP = new CommandValue(CLEANUP.getToken());
    private static final CommandValue CV_EXECUTE = new CommandValue(CommandType.EXECUTE.getToken());
    private static final CommandValue CV_INITIALIZE = new CommandValue(INITIALIZE.getToken());
    private static final CommandValue CV_PLAN = new CommandValue(PLAN.getToken());
    private static final CommandValue CV_RESOURCES = new CommandValue(RESOURCES.getToken());
    private static final CommandValue CV_RESET = new CommandValue(RESET.getToken());
    private static final CommandValue CV_VALIDATE = new CommandValue(CommandType.VALIDATE.getToken());

    private static final Switch K8S_PROXY_URL_SWITCH;
    private static final Switch LIQID_ADDRESS_SWITCH;
    private static final Switch LIQID_GROUP_SWITCH;
    private static final Switch LIQID_PASSWORD_SWITCH;
    private static final Switch LIQID_USERNAME_SWITCH;
    private static final Switch PROCESSORS_SWITCH;
    private static final Switch RESOURCES_SWITCH;
    private static final Switch FORCE_SWITCH;
    private static final Switch LOGGING_SWITCH;
    private static final Switch NO_UPDATE_SWITCH;
    private static final Switch TIMEOUT_SWITCH;
    private static final CommandArgument COMMAND_ARG;

    static {
        try {
            K8S_PROXY_URL_SWITCH =
                new ArgumentSwitch.Builder().setShortName("px")
                                            .setLongName("proxy-url")
                                            .setIsRequired(true)
                                            .addAffinity(CV_CLEANUP).addAffinity(CV_EXECUTE)
                                            .addAffinity(CV_INITIALIZE).addAffinity(CV_RESET)
                                            .addAffinity(CV_PLAN).addAffinity(CV_VALIDATE)
                                            .setValueName("k8x_proxy_url")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies the URL for the kubectl proxy server.")
                                            .build();
            LIQID_ADDRESS_SWITCH =
                new ArgumentSwitch.Builder().setShortName("ip")
                                            .setLongName("liqid-ip-address")
                                            .setIsRequired(true)
                                            .addAffinity(CV_RESOURCES).addAffinity(CV_INITIALIZE).addAffinity(CV_RESET)
                                            .setValueName("ip_address_or_dns_name")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies the URL for the director of the Liqid Cluster.")
                                            .build();
            LIQID_GROUP_SWITCH =
                new ArgumentSwitch.Builder().setShortName("g")
                                            .setLongName("liqid-group")
                                            .setIsRequired(true)
                                            .addAffinity(CV_INITIALIZE)
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
                                            .addAffinity(CV_RESOURCES).addAffinity(CV_INITIALIZE).addAffinity(CV_RESET)
                                            .setValueName("password")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies the password credential for the Liqid Directory.")
                                            .build();
            LIQID_USERNAME_SWITCH =
                new ArgumentSwitch.Builder().setShortName("u")
                                            .setLongName("liqid-username")
                                            .setIsRequired(false)
                                            .addAffinity(CV_RESOURCES).addAffinity(CV_INITIALIZE).addAffinity(CV_RESET)
                                            .setValueName("username")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies the username credential for the Liqid Directory.")
                                            .build();
            LOGGING_SWITCH =
                new SimpleSwitch.Builder().setShortName("l")
                                          .setLongName("logging")
                                          .addDescription("Enables logging for error diagnostics.")
                                          .addDescription("Information will be written to " + com.liqid.k8s.annotate.Application.LOG_FILE_NAME)
                                          .build();
            PROCESSORS_SWITCH =
                new ArgumentSwitch.Builder().setShortName("pr")
                                            .setLongName("processors")
                                            .setValueType(ValueType.STRING)
                                            .setValueName("spec")
                                            .setIsRequired(true)
                                            .setIsMultiple(true)
                                            .addAffinity(CV_INITIALIZE)
                                            .addDescription("List of processor (compute) resources and the corresponding worker node names as known")
                                            .addDescription("to the Kubernetes Cluster.")
                                            .addDescription("{spec} format is:")
                                            .addDescription("  {pcpu_name} ':' {node_name}")
                                            .addDescription("example:")
                                            .addDescription("  -pr=pcpu0:worker1,pcpu1:worker2,pcpu2:worker3")
                                            .build();
            RESOURCES_SWITCH =
                new ArgumentSwitch.Builder().setShortName("r")
                                            .setLongName("resources")
                                            .setValueType(ValueType.STRING)
                                            .setValueName("resource_name")
                                            .setIsRequired(true)
                                            .setIsMultiple(true)
                                            .addAffinity(CV_INITIALIZE)
                                            .addDescription("List of non-compute resources which are to be considered candidates for attaching to")
                                            .addDescription("the compute resources associated with the Kubernetes Cluster.")
                                            .addDescription("format is:")
                                            .addDescription("  {pcpu_name} ':' {node_name}")
                                            .addDescription("example:")
                                            .addDescription("  -r=gpu0,gpu1,gpu2,mem0,mem1,mem2")
                                            .build();
            TIMEOUT_SWITCH =
                new ArgumentSwitch.Builder().setShortName("t")
                                            .setLongName("timeout")
                                            .setIsRequired(false)
                                            .setValueName("seconds")
                                            .setValueType(ValueType.FIXED_POINT)
                                            .addDescription("Timeout value for back-end network communication in seconds.")
                                            .build();
            FORCE_SWITCH =
                new SimpleSwitch.Builder().setShortName("f")
                                          .setLongName("force")
                                          .addAffinity(CV_INITIALIZE).addAffinity(CV_RESET)
                                          .addDescription("Forces command to be executed in spite of certain (not all) detected problems.")
                                          .addDescription("In these cases, the detected problems are flagged as warnings rather than errors.")
                                          .build();
            NO_UPDATE_SWITCH =
                new SimpleSwitch.Builder().setShortName("no")
                                          .setLongName("no-update")
                                          .addAffinity(CV_EXECUTE)
                                          .addDescription("Indicates that no action should be taken; however, the script will display what action")
                                          .addDescription("/would/ be taken in the absence of this switch.")
                                          .build();
            COMMAND_ARG =
                new CommandArgument.Builder().addDescription(CLEANUP.getToken())
                                             .addDescription("  Cleans up the Liqid Configuration.")
                                             .addDescription("  Detaches all resources from the various nodes, then deletes all the machines in the ")
                                             .addDescription("  configured Kubernetes group, returning all resources to the group free pool.")
                                             .addDescription(EXECUTE.getToken())
                                             .addDescription("  Consults the various Kubernetes node annotations, develops a plan to achieve the requested")
                                             .addDescription("  resource layout, then executes the plan.")
                                             .addDescription(PLAN.getToken())
                                             .addDescription("  Consults the various Kubernetes node annotations, develops a plan to achieve the requested")
                                             .addDescription("  resource layout, displays the plan, but does not execute it.")
                                             .addDescription(VALIDATE.getToken())
                                             .addDescription("  Ensures the validity of the Liqid Cluster and Kubernetes Cluster configurations")
                                             .addDescription("    in comparison to the various Kubernetes node annotations.")
                                             .addDescription(RESOURCES.getToken())
                                             .addDescription("  Displays the resources and machines available on the Liqid Cluster.")
                                             .addDescription(INITIALIZE.getToken())
                                             .addDescription("  Configures the Liqid Cluster for use in a Kubernetes Cluster.")
                                             .addDescription("    Creates a resource group if it does not exist.")
                                             .addDescription("    Moves the listed compute nodes into the group, adding the worker node names to the description fields.")
                                             .addDescription("    Moves the listed resources into the group.")
                                             .addDescription("  The Liqid Cluster nodes referenced on the command line must already be configured and running")
                                             .addDescription("  as worker nodes, and should have no resources assigned to them.")
                                             .addDescription(RESET.getToken())
                                             .addDescription("  Entirely resets the configuration of the Liqid Cluster by deleting all groups and machines.")
                                             .addDescription("  Removes all Liqid annotations and other configuration information from the Kubernetes Cluster.")
                                             .addCommandValue(CV_CLEANUP)
                                             .addCommandValue(CV_EXECUTE)
                                             .addCommandValue(CV_INITIALIZE)
                                             .addCommandValue(CV_PLAN)
                                             .addCommandValue(CV_RESOURCES)
                                             .addCommandValue(CV_RESET)
                                             .addCommandValue(CV_VALIDATE)
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
               .addSwitch(LIQID_ADDRESS_SWITCH)
               .addSwitch(LIQID_GROUP_SWITCH)
               .addSwitch(LIQID_USERNAME_SWITCH)
               .addSwitch(LIQID_PASSWORD_SWITCH)
               .addSwitch(LOGGING_SWITCH)
               .addSwitch(K8S_PROXY_URL_SWITCH)
               .addSwitch(TIMEOUT_SWITCH)
               .addSwitch(FORCE_SWITCH)
               .addSwitch(PROCESSORS_SWITCH)
               .addSwitch(RESOURCES_SWITCH)
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
                       .setForce(result._switchSpecifications.containsKey(FORCE_SWITCH))
                       .setNoUpdate(result._switchSpecifications.containsKey(NO_UPDATE_SWITCH))
                       .setLiqidAddress(getSingleString(result._switchSpecifications.get(LIQID_ADDRESS_SWITCH)))
                       .setLiqidGroupName(getSingleString(result._switchSpecifications.get(LIQID_GROUP_SWITCH)))
                       .setLiqidPassword(getSingleString(result._switchSpecifications.get(LIQID_PASSWORD_SWITCH)))
                       .setLiqidUsername(getSingleString(result._switchSpecifications.get(LIQID_USERNAME_SWITCH)))
                       .setProcessorSpecs(getStringCollection(result._switchSpecifications.get(PROCESSORS_SWITCH)))
                       .setResourceSpecs(getStringCollection(result._switchSpecifications.get(RESOURCES_SWITCH)))
                       .setProxyURL(getSingleString(result._switchSpecifications.get(K8S_PROXY_URL_SWITCH)));

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

    //TODO testing
    static String[] tempArgs = {
//        "reset",
//        "-px", "http://192.168.1.220:8001",
//        "-ip", "10.10.14.236",
//        "-f",
//        "-l"

        "initialize",
        "-px", "http://192.168.1.220:8001",
        "-ip", "10.10.14.236",
        "-g", "InspectorMorse",
        "-pr", "pcpu0:kub4",
        "-pr", "pcpu1:kub5",
        "-pr", "pcpu2:kub6",
        "-r", "gpu0,gpu1",
        "-f",
        "-l",
    };
    //TODO end testing

    public static void main(
        final String[] args
    ) {
        var app = new Application();
        if (configureApplication(app, tempArgs)) {
            try {
                app.process();
            } catch (Exception ex) {
                System.out.println("Caught " + ex.getMessage());
                ex.printStackTrace();
            }
        }
    }
}
