/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import org.junit.Test;

import static org.junit.Assert.*;

public class ProfileTest {

    //  TODO Need to test with VendorResourceModel objects
    @Test
    public void injectCount_SimpleGeneric() {
        Integer fpgaCount = 0;
        var fpgaModel = new GenericResourceModel(GeneralType.FPGA);
        Integer gpuCount = 15;
        var gpuModel = new GenericResourceModel(GeneralType.GPU);
        Integer linkCount = -27;
        var linkModel = new GenericResourceModel(GeneralType.LINK);

        var p = new Profile();
        p.injectCount(new GenericResourceModel(GeneralType.FPGA), fpgaCount);
        p.injectCount(new GenericResourceModel(GeneralType.GPU), gpuCount);
        p.injectCount(linkModel, linkCount);

        assertEquals(3, p.getResourceModels().size());
        assertEquals(fpgaCount, p.getCount(fpgaModel));
        assertEquals(gpuCount, p.getCount(gpuModel));
        assertEquals(linkCount, p.getCount(linkModel));
        assertNull(p.getCount(new GenericResourceModel(GeneralType.CPU)));
        assertNull(p.getCount(new SpecificResourceModel(GeneralType.FPGA, "LMI Logic", "LMI1200")));
    }

    @Test
    public void injectCount_SimpleModel() {
        Integer[] counts = {0, 15, -27, 3, 100392, 42};
        ResourceModel[] models = {
            new VendorResourceModel(GeneralType.CPU, "Incel"),
            new SpecificResourceModel(GeneralType.FPGA, "LMI Logic", "LMI1200"),
            new SpecificResourceModel(GeneralType.FPGA, "LMI Logic", "LMI1300"),
            new SpecificResourceModel(GeneralType.FPGA, "NWidia Corpse-aration", "LMI1200"),
            new SpecificResourceModel(GeneralType.GPU, "AMX", "RTX12"),
            new GenericResourceModel(GeneralType.FPGA),
            new GenericResourceModel(GeneralType.GPU),
            };

        var p = new Profile();
        for (var x = 0; x < counts.length; x++) {
            p.injectCount(models[x], counts[x]);
        }

        assertEquals(counts.length, p.getResourceModels().size());
        for (var x = 0; x < counts.length; x++) {
            assertEquals(counts[x], p.getCount(models[x]));
        }
    }

    @Test
    public void get_Varieties() {
        var p = new Profile();
        p.injectCount(new GenericResourceModel(GeneralType.CPU), 5);
        p.injectCount(new VendorResourceModel(GeneralType.LINK, "3Com"), 4);
        p.injectCount(new SpecificResourceModel(GeneralType.CPU, "Motorola", "M68000"), 1);
        p.injectCount(new SpecificResourceModel(GeneralType.SSD, "Liqid", "HoneyBadger"), 27);

        assertEquals((Integer)5, p.getCount(new GenericResourceModel(GeneralType.CPU)));
        assertEquals((Integer)4, p.getCount(new VendorResourceModel(GeneralType.LINK, "3Com")));
        assertEquals((Integer)1, p.getCount(new SpecificResourceModel(GeneralType.CPU, "Motorola", "M68000")));
        assertNull(p.getCount(new GenericResourceModel(GeneralType.SSD)));
        assertEquals((Integer)27, p.getCount(new SpecificResourceModel(GeneralType.SSD, "Liqid", "HoneyBadger")));
    }

    @Test
    public void getResourceModels() {
        ResourceModel[] models = {
            new GenericResourceModel(GeneralType.SSD),
            new GenericResourceModel(GeneralType.MEMORY),
            new GenericResourceModel(GeneralType.LINK),
            new GenericResourceModel(GeneralType.CPU),
            new SpecificResourceModel(GeneralType.CPU, "Zilog", "Z80"),
        };

        var p = new Profile();
        for (var model : models) {
            p.injectCount(model, 0);
        }

        var resModels = p.getResourceModels();
        assertEquals(models.length, resModels.size());
        for (var model : models) {
            assertTrue(resModels.contains(model));
        }
    }

    @Test
    public void injectCount_Sums() {
        var p = new Profile();
        p.injectCount(new GenericResourceModel(GeneralType.CPU), 0);
        p.injectCount(new GenericResourceModel(GeneralType.CPU), 15);
        p.injectCount(new GenericResourceModel(GeneralType.CPU), -3);
        p.injectCount(new GenericResourceModel(GeneralType.FPGA), -12);

        assertEquals((Integer)12, p.getCount(new GenericResourceModel(GeneralType.CPU)));
        assertEquals((Integer)(-12), p.getCount(new GenericResourceModel(GeneralType.FPGA)));
    }
}
