package org.fiware.mintaka.persistence;


import lombok.Data;

import javax.persistence.*;
import java.io.Serializable;
import java.math.BigInteger;
import java.time.LocalDateTime;

/**
 * Database representation of a concrete Attribute. Schema is defined by <a href="https://github.com/FIWARE/context.Orion-LD">Orion-LD</a>
 */
@Data
@Entity
@Table(name = "attributes")
// Due to a shortcomming of hibernate, that mapping has to be defined at a class annotaded with @Entity to be picked up. It would fit better at its
// targetclass, but thats not an entity
@SqlResultSetMapping(
		name = "AggregatedAttributeMapping",
		classes = {
				@ConstructorResult(
						targetClass = AggregatedAttribute.class,
						columns = {
								@ColumnResult(name = "time_bucket", type = LocalDateTime.class),
								@ColumnResult(name = "count", type = BigInteger.class),
								@ColumnResult(name = "distinct_count", type = BigInteger.class),
								@ColumnResult(name = "avg", type = Double.class),
								@ColumnResult(name = "sum", type = Double.class),
								@ColumnResult(name = "stddev", type = Double.class),
								@ColumnResult(name = "sumsq", type = Double.class),
								@ColumnResult(name = "min", type = String.class),
								@ColumnResult(name = "max", type = String.class)
						}
				)
		}
)
public class Attribute extends AbstractAttribute {

	@Enumerated(EnumType.STRING)
	@Column(name = "opmode")
	private OpMode opMode;

	@Column(name = "datasetid")
	private String datasetId;

	@Column(name = "subproperties")
	private Boolean subProperties;
}
