package org.fiware.mintaka.persistence;

import lombok.Data;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.time.Instant;

/**
 * Database representation of a concrete Entity. Schema is defined by <a href="https://github.com/FIWARE/context.Orion-LD">Orion-LD</a>
 */
@Data
@Entity
@Table(name = "entities")
public class NgsiEntity {

    @NotNull
    @Id
    @Column(name = "instanceid")
    private String instanceId;

    @NotNull
    private String id;

    private String type;

    @Enumerated(EnumType.STRING)
    @Column(name = "opmode")
    private OpMode opMode;

    @NotNull
    private Instant ts;
}
