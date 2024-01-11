/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

public class UnassignedLayout {

    private final Profile _profile;

    public UnassignedLayout() {
        _profile = new Profile();
    }

    public Profile getProfile() { return _profile; }

    public void show(
        final String indent
    ) {
        System.out.println(indent + "<unassigned>");
        _profile.show(indent + "  ");
    }
}
