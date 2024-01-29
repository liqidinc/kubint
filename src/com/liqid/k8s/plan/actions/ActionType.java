/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.plan.actions;

public enum ActionType {
    ANNOTATE_NODE,
    ASSIGN_RESOURCES_TO_GROUP,
    ASSIGN_RESOURCES_TO_MACHINE,
    CLEAR_CONFIGURATION,
    CREATE_GROUP,
    CREATE_LINKAGE,
    CREATE_MACHINE,
    DELETE_GROUP,
    DELETE_MACHINE,
    ENABLE_P2P_FOR_MACHINE,
    NO_OPERATION,
    RECONFIGURE_MACHINE,
    REMOVE_ALL_ANNOTATIONS,
    REMOVE_ANNOTATIONS,
    REMOVE_LINKAGE,
    REMOVE_RESOURCES_FROM_GROUP,
    REMOVE_RESOURCES_FROM_MACHINE,
    SET_USER_DESCRIPTION,
}
