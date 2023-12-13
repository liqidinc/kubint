/**
 * k8s-integration
 * Copyright 2023 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s;

public class Constants {

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
