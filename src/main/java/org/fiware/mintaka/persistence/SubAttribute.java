package org.fiware.mintaka.persistence;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;

/**
 * Database representation of a concrete SubAttribute. Schema is defined by orion-ld
 * {@see https://github.com/FIWARE/context.Orion-LD}
 */
@Data
@Entity
@Table(name = "subattributes")
public class SubAttribute extends AbstractAttribute {

	@NotNull
	@Column(name = "attrinstanceid")
	private String attributeInstanceId;
}
