/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import java.util.HashMap;

public class TypeProfile extends HashMap<ResourceModel, Integer> {

    TypeProfile() {}

    public void show(
        final String indent
    ) {
        for (var e : entrySet()) {
            System.out.printf("%s%s=%d\n", indent, e.getKey().toString(), e.getValue());
        }
    }
}
