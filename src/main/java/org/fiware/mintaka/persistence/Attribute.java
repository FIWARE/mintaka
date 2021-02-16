package org.fiware.mintaka.persistence;


import lombok.Data;

import javax.persistence.*;

/**
 * Database representation of a concrete Attribute. Schema is defined by orion-ld
 * {@see https://github.com/FIWARE/context.Orion-LD}
 */
@Data
@Entity
@Table(name = "attributes")
public class Attribute extends AbstractAttribute{

    @Enumerated(EnumType.STRING)
    @Column(name = "opmode")
    private OpMode opMode;

    @Column(name = "subproperties")
    private Boolean subProperties;
}
