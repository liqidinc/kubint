/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s;

import com.bearsnake.k8sclient.K8SException;
import com.bearsnake.k8sclient.K8SHTTPError;
import com.bearsnake.k8sclient.K8SJSONError;
import com.bearsnake.k8sclient.K8SRequestError;
import com.bearsnake.klog.FileWriter;
import com.bearsnake.klog.Level;
import com.bearsnake.klog.LevelMask;
import com.bearsnake.klog.Logger;
import com.bearsnake.klog.PrefixEntity;
import com.bearsnake.klog.StdOutWriter;
import com.bearsnake.komando.ArgumentSwitch;
import com.bearsnake.komando.CommandArgument;
import com.bearsnake.komando.CommandLineHandler;
import com.bearsnake.komando.Result;
import com.bearsnake.komando.SimpleSwitch;
import com.bearsnake.komando.Switch;
import com.bearsnake.komando.exceptions.KomandoException;
import com.bearsnake.komando.values.CommandValue;
import com.bearsnake.komando.values.FixedPointValue;
import com.bearsnake.komando.values.StringValue;
import com.bearsnake.komando.values.Value;
import com.bearsnake.komando.values.ValueType;
import com.liqid.k8s.commands.CommandType;
import com.liqid.k8s.exceptions.ConfigurationDataException;
import com.liqid.k8s.exceptions.ConfigurationException;
import com.liqid.k8s.exceptions.InternalErrorException;
import com.liqid.k8s.exceptions.ProcessingException;
import com.liqid.sdk.LiqidException;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static com.liqid.k8s.commands.CommandType.*;
import static com.liqid.k8s.config.CommandType.RESET;

/*
    adopt TODO
        -px,--proxy-url={proxy_url}
        -pr,--processors={pcpu_name=worker_node_name}[,...]
        -r,--resources={name}[,...]
        [ -f,--force ]
        [ -no,--no-update ]

    compose TODO
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

    label TODO
        -px,--proxy-url={proxy_url}
        -a,--automatic
        -n,--worker-node={worker_node_name}
        [ --clear ]
        [ -m,--liqid-machine={liqid_machine} ]
        [ -fs,--fpga-spec={spec}[,...] ]
        [ -gs,--gpu-spec={spec}[,...] ]
        [ -ls,--link-spec={spec}[,...] ]
        [ -ms,--mem-spec={spec}[,...] ]
        [ -ss,--ssd-spec={spec}[,...] ]
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

    release TODO
        -px,--proxy-url={proxy_url}
        -pr,--processors={pcpu_name}[,...]
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
    /*
    OLD ANNOTATE COMMANDS
    annotate auto
        -px,--proxy-url={proxy_url}
        [ -no,--no-update ]
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
        [ -n,--worker-node={worker_node_name} ]
        [ -a ]
     */


    /*
    OLD CONFIG COMMANDS
    config cleanup
        -px,--proxy-url={proxy_url}

    config execute
        -px,--proxy-url={proxy_url}

    config reset
        -px,--proxy-url={proxy_url}
        -ip,--liqid-ip-address={ip_address}
        [ -u,--liqid-username={user_name} ]
        [ -p,--liqid-password={password} ]
        -f,--force

    config validate
        -px,--proxy-url={proxy_url}
    */

    private static final String LOGGER_NAME = "Config";
    private static final String LOG_FILE_NAME = "liq-config.log";

    private static final CommandValue CV_INITIALIZE = new CommandValue(INITIALIZE.getToken());
    private static final CommandValue CV_LINK = new CommandValue(LINK.getToken());
    private static final CommandValue CV_NODES = new CommandValue(NODES.getToken());
    private static final CommandValue CV_RESET = new CommandValue(RESET.getToken());
    private static final CommandValue CV_RESOURCES = new CommandValue(RESOURCES.getToken());
    private static final CommandValue CV_UNLINK = new CommandValue(UNLINK.getToken());

    private static final CommandArgument COMMAND_ARG;
    private static final Switch ALLOCATE_SWITCH;
    private static final Switch FORCE_SWITCH;
    private static final Switch LIQID_ADDRESS_SWITCH;
    private static final Switch LIQID_GROUP_SWITCH;
    private static final Switch LIQID_PASSWORD_SWITCH;
    private static final Switch LIQID_USERNAME_SWITCH;
    private static final Switch LOGGING_SWITCH;
    private static final Switch NO_UPDATE_SWITCH;
    private static final Switch PROCESSORS_SWITCH;
    private static final Switch PROXY_URL_SWITCH;
    private static final Switch RESOURCES_SWITCH;
    private static final Switch TIMEOUT_SWITCH;

    static {
        try {
            ALLOCATE_SWITCH =
                new SimpleSwitch.Builder().setShortName("al")
                                          .setLongName("allocate")
                                          .addAffinity(CV_INITIALIZE)
                                          .addDescription("Causes the initialize process to create annotations and subsequently")
                                          .addDescription("allocate resources to worker nodes, as equally as possible per type.")
                                          .build();
            FORCE_SWITCH =
                new SimpleSwitch.Builder().setShortName("f")
                                          .setLongName("force")
                                          .addAffinity(CV_INITIALIZE)
                                          .addAffinity(CV_LINK)
                                          .addAffinity(CV_RESET)
                                          .addAffinity(CV_UNLINK)
                                          .addDescription("Forces command to be executed in spite of certain (not all) detected problems.")
                                          .addDescription("In these cases, the detected problems are flagged as warnings rather than errors.")
                                          .build();
            PROXY_URL_SWITCH =
                new ArgumentSwitch.Builder().setShortName("px")
                                            .setLongName("proxy-url")
                                            .setIsRequired(true)
                                            .addAffinity(CV_INITIALIZE)
                                            .addAffinity(CV_LINK)
                                            .addAffinity(CV_NODES)
                                            .addAffinity(CV_RESET)
                                            .addAffinity(CV_UNLINK)
                                            .setValueName("k8x_proxy_url")
                                            .setValueType(ValueType.STRING)
                                            .addDescription("Specifies the URL for the kubectl proxy server.")
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
            LOGGING_SWITCH =
                new SimpleSwitch.Builder().setShortName("l")
                                          .setLongName("logging")
                                          .addDescription("Enables logging for error diagnostics.")
                                          .addDescription("Information will be written to " + LOG_FILE_NAME)
                                          .build();
            NO_UPDATE_SWITCH =
                new SimpleSwitch.Builder().setShortName("no")
                                          .setLongName("no-update")
                                          .addAffinity(CV_INITIALIZE)
                                          .addAffinity(CV_LINK)
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
            COMMAND_ARG =
                new CommandArgument.Builder().addDescription(INITIALIZE.getToken())
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
                                             .addDescription(RESET.getToken())
                                             .addDescription("  Entirely resets the configuration of the Liqid Cluster by deleting all groups and machines.")
                                             .addDescription("  Removes all Liqid annotations and other configuration information from the Kubernetes Cluster.")
                                             .addDescription(RESOURCES.getToken())
                                             .addDescription("  Displays the resources and machines available on the Liqid Cluster.")
                                             .addDescription(UNLINK.getToken())
                                             .addDescription("  Unlinks a particular Liqid Cluster from the targeted Kubernetes Cluster.")
                                             .addDescription("  Removes the Liqid Cluster information provided via the " + LINK.getToken() + " command (listed above).")
                                             .addDescription("  Cannot be invoked so long as there are any existing node->machine labels.")
                                             .addCommandValue(CV_INITIALIZE)
                                             .addCommandValue(CV_LINK)
                                             .addCommandValue(CV_NODES)
                                             .addCommandValue(CV_RESET)
                                             .addCommandValue(CV_RESOURCES)
                                             .addCommandValue(CV_UNLINK)
                                             .build();
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
                                   .setForce(result._switchSpecifications.containsKey(FORCE_SWITCH))
                                   .setLiqidAddress(getSingleString(result._switchSpecifications.get(LIQID_ADDRESS_SWITCH)))
                                   .setLiqidGroupName(getSingleString(result._switchSpecifications.get(LIQID_GROUP_SWITCH)))
                                   .setLiqidPassword(getSingleString(result._switchSpecifications.get(LIQID_PASSWORD_SWITCH)))
                                   .setLiqidUsername(getSingleString(result._switchSpecifications.get(LIQID_USERNAME_SWITCH)))
                                   .setLogger(_logger)
                                   .setNoUpdate(result._switchSpecifications.containsKey(NO_UPDATE_SWITCH))
                                   .setProcessorSpecs(getStringCollection(result._switchSpecifications.get(PROCESSORS_SWITCH)))
                                   .setProxyURL(getSingleString(result._switchSpecifications.get(PROXY_URL_SWITCH)))
                                   .setResourceSpecs(getStringCollection(result._switchSpecifications.get(RESOURCES_SWITCH)));

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
           .addSwitch(LIQID_ADDRESS_SWITCH)
           .addSwitch(LIQID_GROUP_SWITCH)
           .addSwitch(LIQID_USERNAME_SWITCH)
           .addSwitch(LIQID_PASSWORD_SWITCH)
           .addSwitch(LOGGING_SWITCH)
           .addSwitch(TIMEOUT_SWITCH)
           .addSwitch(FORCE_SWITCH)
           .addSwitch(NO_UPDATE_SWITCH)
           .addSwitch(PROCESSORS_SWITCH)
           .addSwitch(PROXY_URL_SWITCH)
           .addSwitch(RESOURCES_SWITCH)
           .addCommandArgument(COMMAND_ARG);

        var result = clh.processCommandLine(args);
        if (result.hasErrors() || result.hasWarnings()) {
            for (var msg : result._messages) {
                System.err.println(msg);
            }
            System.err.println("Use --help for usage assistance");
            if (result.hasErrors()) {
                return null;
            }
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
                _logger.catching(ex);
                System.err.println("Configuration inconsistency(ies) prevent further processing.");
                System.err.println("Please collect logging information and contact Liqid Support.");
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
