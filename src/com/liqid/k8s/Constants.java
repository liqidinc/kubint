/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s;

public class Constants {

    /*
    We deal in ConfigMaps, Secrets, and Annotations.

    Firstly, we have the concept of a linkage between the Kubernetes Cluster and the Liqid Cluster.
    This link is defined by a ConfigMap and optionally a Secret.
    The ConfigMap exists in namespace K8S_CONFIG_NAMESPACE with name K8S_CONFIG_NAME, and has two entries:
        K8S_CONFIG_MAP_IP_ADDRESS_KEY is the key for the IP address of the Liqid Cluster Director
        K8S_CONFIG_MAP_GROUP_NAME_KEY is the key for the name of the Liqid Cluster group where we put all the
            machines and resources that we're going to manage for the Kubernetes Cluster.

    Note that this allows us to observe multi-tenancy. Any resources which are in other groups, or which are
    not in any group, are generally left alone. This allows other clients (including other Kubernetes clusters)
    to work with resources in the Liqid Cluster. Particularly, we can have multiple Kubernetes Clusters associated
    with any particular Liqid Cluster - each one associated with a separate group.

    The Secret is optional, only being required if the Liqid Cluster has authentication enabled.
    If it does, then the secret exists in namespace K8S_SECRET_NAMESPACE with name K8S_SECRET_NAME.
    It has a single entry with key K8S_SECRET_CREDENTIAL_KEY of type "Opaque". It consists of a base64 encoding of
    '{username}' or '{username}:{password}'.
    This is not particularly secure; we really should encrypt the thing instead, using some aspect of the cluster
    as the encryption key.

    Secondly, we have the concept of labeling, which is actually annotating in Kubernetes terms.
    Every worker node which we manage has annotations with a prefix of K8S_ANNOTATION_PREFIX.
    The following annotation keys are always present:
        K8S_ANNOTATION_MACHINE_NAME - the value of this is the name of the associated machine for the worker node.
            This implicitly ties (or should tie) the worker node to the CPU resource, since said resource is
            (or should be) assigned to the referenced machine.
        K8S_ANNOTATION_FPGA_ENTRY - the value of this indicates the number of FPGA resources to be allocated to
            this node. It is formatted like so:
                {entry}[,...]
            where {entry} is formatted like so:
                [{vendor} ':' {model} ':'] {number}
            where {model} is a model of FPGA as reported by Liqid (and the various scripts in this project).
              and {n} is the number of such models of FPGAs to be allocated.
            If model is not specified, then the number allows for any particular model to be assigned.
            Examples:
                "5"
                    Five FPGAs will be allocated, without respect to vendor or model.
                "Acme:CPU8080:3"
                    Three FPGAs of model CPU8080 from vendor Acme will be allocated.
                "Acme:CPU6502:2,Acme:CPU1802:1,3"
                    2 FPGAs of model CPU6502, 1 FPGA of model CPU1802, and 3 other FPGAs of any model will be allocated.
            If there is not an entry for this key, then no FPGAs will be allocated to this node.
        K8S_ANNOTATION_GPU_ENTRY - as above, but for GPU resources
        K8S_ANNOTATION_LINK_ENTRY - as above, but for LINK (i.e., network) resources
        K8S_ANNOTATION_MEMORY_ENTRY - as above, but for MEM (i.e., NVMe memory) resources
        K8S_ANNOTATION_SSD_ENTRY - as above, but for PCI-based storage resources (generally SSDs)
     */

    public static final String K8S_ANNOTATION_PREFIX = "kubint.liqid.com";
    public static final String K8S_ANNOTATION_MACHINE_NAME = "machine-name";
    public static final String K8S_ANNOTATION_FPGA_ENTRY = "fpga-resources";
    public static final String K8S_ANNOTATION_GPU_ENTRY = "gpu-resources";
    public static final String K8S_ANNOTATION_LINK_ENTRY = "link-resources";
    public static final String K8S_ANNOTATION_MEMORY_ENTRY = "memory-resources";
    public static final String K8S_ANNOTATION_SSD_ENTRY = "ssd-resources";

    public static final String[] K8S_ANNOTATION_KEYS = {
        K8S_ANNOTATION_MACHINE_NAME,
        K8S_ANNOTATION_FPGA_ENTRY,
        K8S_ANNOTATION_GPU_ENTRY,
        K8S_ANNOTATION_LINK_ENTRY,
        K8S_ANNOTATION_MEMORY_ENTRY,
        K8S_ANNOTATION_SSD_ENTRY,
    };

    public static final String K8S_CONFIG_NAME = "kubint.liqid.com";
    public static final String K8S_CONFIG_NAMESPACE = "default";
    public static final String K8S_CONFIG_MAP_IP_ADDRESS_KEY = "address";
    public static final String K8S_CONFIG_MAP_GROUP_NAME_KEY = "group";

    public static final String K8S_SECRET_NAME = "kubint.liqid.com";
    public static final String K8S_SECRET_NAMESPACE = "default";
    public static final String K8S_SECRET_CREDENTIALS_KEY = "credentials";

    public static final String LIQID_SDK_LABEL = "KubInt";

    public static final String VERSION = "3.0";
}
