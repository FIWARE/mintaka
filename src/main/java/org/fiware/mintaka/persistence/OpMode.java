package org.fiware.mintaka.persistence;

/**
 * OpMode enum as used by orion-ld
 * {@see https://github.com/FIWARE/context.Orion-LD}
 */
public enum OpMode {
	Create("Create"),
	Append("Append"),
	Update("Update"),
	Replace("Replace"),
	Delete("Delete");

	private final String name;

	OpMode(String name) {
		this.name = name;
	}
}
