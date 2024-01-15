/**
 * k8s-integration
 * Copyright 2023-2024 by Liqid, Inc - All Rights Reserved
 */

package com.liqid.k8s.layout;

import org.junit.Test;

import static org.junit.Assert.*;

public class ProfileTest {

    @Test
    public void injectCount_SimpleGeneric() {
        Integer fpgaCount = 0;
        var fpgaModel = new ResourceModel(GeneralType.FPGA);
        Integer gpuCount = 15;
        var gpuModel = new ResourceModel(GeneralType.GPU);
        Integer linkCount = -27;
        var linkModel = new ResourceModel(GeneralType.LINK);

        var p = new Profile();
        p.injectCount(GeneralType.FPGA, fpgaCount);
        p.injectCount(GeneralType.GPU, gpuCount);
        p.injectCount(linkModel, linkCount);

        assertEquals(3, p.getResourceModels().size());
        assertEquals(fpgaCount, p.getCount(fpgaModel));
        assertEquals(gpuCount, p.getCount(gpuModel));
        assertEquals(linkCount, p.getCount(linkModel));
        assertNull(p.getCount(new ResourceModel(GeneralType.CPU)));
        assertNull(p.getCount(new ResourceModel(GeneralType.FPGA, "LMI Logic", "LMI1200")));
    }

    @Test
    public void injectCount_SimpleModel() {
        Integer[] counts = {0, 15, -27, 3, 100392, 42};
        ResourceModel[] models = {
            new ResourceModel(GeneralType.FPGA, "LMI Logic", "LMI1200"),
            new ResourceModel(GeneralType.FPGA, "LMI Logic", "LMI1300"),
            new ResourceModel(GeneralType.FPGA, "NWidia Corpse-aration", "LMI1200"),
            new ResourceModel(GeneralType.GPU, "AMX", "RTX12"),
            new ResourceModel(GeneralType.FPGA),
            new ResourceModel(GeneralType.GPU),
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
    public void injectCount_SimpleSpecific() {
        Integer[] counts = {0, 15, -27, 3, 100392, 42};
        ResourceModel[] models = {
            new ResourceModel(GeneralType.FPGA, "LMI Logic", "LMI1200"),
            new ResourceModel(GeneralType.FPGA, "LMI Logic", "LMI1300"),
            new ResourceModel(GeneralType.FPGA, "NWidia Corpse-aration", "LMI1200"),
            new ResourceModel(GeneralType.GPU, "AMX", "RTX12"),
            new ResourceModel(GeneralType.FPGA),
            new ResourceModel(GeneralType.GPU),
            };

        var p = new Profile();
        for (var x = 0; x < counts.length; x++) {
            p.injectCount(models[x].getGeneralType(),
                          models[x].getVendorName(),
                          models[x].getModelName(),
                          counts[x]);
        }

        assertEquals(counts.length, p.getResourceModels().size());
        for (var x = 0; x < counts.length; x++) {
            assertEquals(counts[x], p.getCount(models[x]));
        }
    }

    @Test
    public void get_Varieties() {
        var p = new Profile();
        p.injectCount(GeneralType.CPU, 5);
        p.injectCount(GeneralType.CPU, "Motorola", "M68000", 1);
        p.injectCount(GeneralType.SSD, "Liqid", "HoneyBadger", 27);

        assertEquals((Integer)5, p.getCount(GeneralType.CPU));
        assertEquals((Integer)5, p.getCount(GeneralType.CPU, null, null));
        assertEquals((Integer)5, p.getCount(new ResourceModel(GeneralType.CPU)));
        assertEquals((Integer)5, p.getCount(new ResourceModel(GeneralType.CPU, null, null)));

        assertEquals((Integer)1, p.getCount(GeneralType.CPU, "Motorola", "M68000"));
        assertEquals((Integer)1, p.getCount(new ResourceModel(GeneralType.CPU, "Motorola", "M68000")));

        assertNull(p.getCount(GeneralType.SSD));
        assertNull(p.getCount(GeneralType.SSD, null, null));
        assertNull(p.getCount(new ResourceModel(GeneralType.SSD)));
        assertNull(p.getCount(new ResourceModel(GeneralType.SSD, null, null)));

        assertEquals((Integer)27, p.getCount(GeneralType.SSD, "Liqid", "HoneyBadger"));
        assertEquals((Integer)27, p.getCount(new ResourceModel(GeneralType.SSD, "Liqid", "HoneyBadger")));
    }

    @Test
    public void getResourceModels() {
        ResourceModel[] models = {
            new ResourceModel(GeneralType.SSD),
            new ResourceModel(GeneralType.MEMORY),
            new ResourceModel(GeneralType.LINK),
            new ResourceModel(GeneralType.CPU),
            new ResourceModel(GeneralType.CPU, "Zilog", "Z80"),
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
        p.injectCount(GeneralType.CPU, 0);
        p.injectCount(GeneralType.CPU, 15);
        p.injectCount(GeneralType.CPU, -3);
        p.injectCount(GeneralType.FPGA, -12);

        assertEquals((Integer)12, p.getCount(GeneralType.CPU));
        assertEquals((Integer)(-12), p.getCount(GeneralType.FPGA));
    }
}
