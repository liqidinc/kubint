/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.config;

import com.bearsnake.komando.*;
import com.bearsnake.komando.exceptions.*;
import com.bearsnake.komando.restrictions.EnumerationRestriction;
import com.bearsnake.komando.values.*;

import java.util.List;

import static com.liqid.k8s.config.Application.LOG_FILE_NAME;

public class Main {

    /*
    config [ plan | execute ]
        -px,--proxy-url={proxy_url}

    config validate
        -px,--proxy-url={proxy_url}

    config resources
        -ip,--liqid-ip-address={ip_address}
        [ -u,--liqid-username={user_name} ]
        [ -p,--liqid-password={password} ]

    config nodes
        -px,--proxy-url={proxy_url}

    config initialize
        -ip,--liqid-ip-address={ip_address}
        [ -u,--liqid-username={user_name} ]
        [ -p,--liqid-password={password} ]
        -g,--liqid-group={group_name}
        -r,--resources {resource_name}[,...]
        -f,--force

    config cleanup
        -ip,--liqid-ip-address={ip_address}
        [ -u,--liqid-username={user_name} ]
        [ -p,--liqid-password={password} ]
        -g,--liqid-group={group_name}
        -f,--force
    */

    public static final String VERSION = "3.0";
//    public static final String LIQID_K8S_GROUP_NAME = "Kubernetes";
//    public static final String LIQID_MACHINE_NAME_PREFIX = "Kubernetes-";

//    private static final Switch CLEAR_CONFIG_SWITCH;
//    private static final Switch DIRECTOR_ADDRESS_SWITCH;
//    private static final Switch DIRECTOR_PASSWORD_SWITCH;
//    private static final Switch DIRECTOR_USERNAME_SWITCH;
    private static final Switch LOGGING_SWITCH;
//    private static final Switch PROXY_URL_SWITCH;
//    private static final Switch SHOW_SWITCH;
//    private static final Switch TIMEOUT_SWITCH;
//    private static final PositionalArgument COMMAND_ARG;

    static {
        try {
//            CLEAR_CONFIG_SWITCH =
//                new ArgumentSwitch.Builder().setShortName("ccfg")
//                                            .setIsMultiple(false)
//                                            .setIsRequired(false)
//                                            .setValueName("confirmation")
//                                            .setValueType(ValueType.STRING)
//                                            .addDescription("Indicates that the Liqid Cluster configuration should be reset")
//                                            .addDescription("prior to initiating a CONFIG operation.")
//                                            .addDescription("The confirmation string must be \"YES\".")
//                                            .setRestriction(new EnumerationRestriction(new String[]{"YES"}))
//                                            .build();
//            DIRECTOR_ADDRESS_SWITCH =
//                new ArgumentSwitch.Builder().setShortName("ip")
//                                            .setLongName("ip-address")
//                                            .setIsRequired(true)
//                                            .setValueName("dns_or_ip_address")
//                                            .setValueType(ValueType.STRING)
//                                            .addDescription("Specifies the URL for the director of the Liqid Cluster.")
//                                            .build();
//            DIRECTOR_PASSWORD_SWITCH =
//                new ArgumentSwitch.Builder().setShortName("p")
//                                            .setLongName("password")
//                                            .setIsRequired(true)
//                                            .setValueName("password")
//                                            .setValueType(ValueType.STRING)
//                                            .addDescription("Specifies the password credential for the Liqid Directory.")
//                                            .build();
//            DIRECTOR_USERNAME_SWITCH =
//                new ArgumentSwitch.Builder().setShortName("u")
//                                            .setLongName("username")
//                                            .setIsRequired(true)
//                                            .setValueName("username")
//                                            .setValueType(ValueType.STRING)
//                                            .addDescription("Specifies the username credential for the Liqid Directory.")
//                                            .build();
            LOGGING_SWITCH =
                new SimpleSwitch.Builder().setShortName("l")
                                          .setLongName("logging")
                                          .addDescription("Enables logging for error diagnostics.")
                                          .addDescription("Information will be written to " + LOG_FILE_NAME)
                                          .build();
//            PROXY_URL_SWITCH =
//                new ArgumentSwitch.Builder().setShortName("px")
//                                            .setLongName("proxy-url")
//                                            .setIsRequired(true)
//                                            .setValueName("k8x_proxy_url")
//                                            .setValueType(ValueType.STRING)
//                                            .addDescription("Specifies the URL for the kubectl proxy server.")
//                                            .build();
//            TIMEOUT_SWITCH =
//                new ArgumentSwitch.Builder().setShortName("t")
//                                            .setLongName("timeout")
//                                            .setIsRequired(false)
//                                            .setValueName("seconds")
//                                            .setValueType(ValueType.FIXED_POINT)
//                                            .addDescription("Timeout value for back-end network communication in seconds.")
//                                            .build();
//            SHOW_SWITCH =
//                new SimpleSwitch.Builder().setShortName("s")
//                                          .setLongName("show")
//                                          .addDescription("Shows the plan derived by the script without executing it.")
//                                          .build();
//            COMMAND_ARG =
//                new PositionalArgument.Builder().setValueName("command")
//                                                .setValueType(ValueType.STRING)
//                                                .addDescription("config")
//                                                .addDescription("  Applies the configuration defined in the config file")
//                                                .addDescription("  to the Liqid cluster. Creates a Liqid group named \"Kubernetes\"")
//                                                .addDescription("  if it does not exist. Creates Liqid machines for the nodes which")
//                                                .addDescription("  do not already have machines. Applies resources as requested for")
//                                                .addDescription("  the newly-assigned nodes.")
//                                                .addDescription("expand")
//                                                .addDescription("  Identifies nodes in the configuration file which are not part")
//                                                .addDescription("  of the group in the Kubernetes Liqid cluster, and adds them thereto,")
//                                                .addDescription("  assigning requested resources if possible.")
//                                                .addDescription("update")
//                                                .addDescription("  Detaches and re-attaches resources to the various configured machines")
//                                                .addDescription("  according to the given configuration while attempting to avoid")
//                                                .addDescription("  unnecessary disruption to the Kubernetes deployment.")
//                                                .setIsRequired(true)
//                                                .setRestriction(new EnumerationRestriction(Application.COMMAND_LIST))
//                                                .build();
        } catch (KomandoException e) {
            throw new RuntimeException(e);
        }
    }

    // ------------------------------------------------------------------------
    // helper functions
    // ------------------------------------------------------------------------

//    private static String getSingleStringValue(
//        final List<Value> valueList
//    ) {
//        String result = null;
//        if ((valueList != null) && !valueList.isEmpty()) {
//            result = ((StringValue)valueList.get(0)).getValue();
//        }
//        return result;
//    }

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
//               .addSwitch(CLEAR_CONFIG_SWITCH)
//               .addSwitch(DIRECTOR_ADDRESS_SWITCH)
//               .addSwitch(DIRECTOR_USERNAME_SWITCH)
//               .addSwitch(DIRECTOR_PASSWORD_SWITCH)
               .addSwitch(LOGGING_SWITCH);
//               .addSwitch(PROXY_URL_SWITCH)
//               .addSwitch(SHOW_SWITCH)
//               .addSwitch(TIMEOUT_SWITCH)
//               .addPositionalArgument(COMMAND_ARG);

            var result = clh.processCommandLine(args);
            if (result.hasErrors() || result.hasWarnings()) {
                for (var msg : result._messages) {
                    System.err.println(msg);
                }
                System.err.println("Use --help for usage assistance");
                return false;
            } else if (result.isHelpRequested()) {
                clh.displayUsage("");
                return false;
            } else if (result.isVersionRequested()) {
                System.out.println("k8sIntegration Version " + VERSION);
                return false;
            }

            application.setLogging(result._switchSpecifications.containsKey(LOGGING_SWITCH));
//                       .setClearContext(result._switchSpecifications.containsKey(CLEAR_CONFIG_SWITCH))
//                       .setCommand(((StringValue)result._positionalArgumentSpecifications.get(0)).getValue())
//                       .setDirectorAddress(getSingleStringValue(result._switchSpecifications.get(DIRECTOR_ADDRESS_SWITCH)))
//                       .setDirectorPassword(getSingleStringValue(result._switchSpecifications.get(DIRECTOR_USERNAME_SWITCH)))
//                       .setDirectorUsername(getSingleStringValue(result._switchSpecifications.get(DIRECTOR_PASSWORD_SWITCH)))
//                       .setShowMode(result._switchSpecifications.containsKey(SHOW_SWITCH))
//                       .setProxyURL(getSingleStringValue(result._switchSpecifications.get(PROXY_URL_SWITCH)));

//            var values = result._switchSpecifications.get(TIMEOUT_SWITCH);
//            if ((values != null) && !values.isEmpty()) {
//                application.setTimeoutInSeconds((int) (long) ((FixedPointValue) values.get(0)).getValue());
//            }

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
        var app = new Application();
        if (configureApplication(app, args)) {
            try {
                app.process();
            } catch (Exception ex) {
                System.out.println("Caught " + ex.getMessage());
                ex.printStackTrace();
            }
        }
    }
}
