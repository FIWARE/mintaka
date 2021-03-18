package org.fiware.mintaka.persistence;


import lombok.Data;

import javax.persistence.*;

/**
 * Database representation of a concrete Attribute. Schema is defined by <a href="https://github.com/FIWARE/context.Orion-LD">Orion-LD</a>
 */
@Data
@Entity
@Table(name = "attributes")
public class Attribute extends AbstractAttribute{

    @Enumerated(EnumType.STRING)
    @Column(name = "opmode")
    private OpMode opMode;

    @Column(name = "datasetid")
    private String datasetId;

    @Column(name = "subproperties")
    private Boolean subProperties;
}
