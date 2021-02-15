package org.fiware.mintaka.persistence;

public enum OpMode {
	Create("Create"),
	Append("Append"),
	Update("Update"),
	Replace("Replace"),
	Delete("Delete");

	private final String name;

	private OpMode(String name) {
		this.name = name;
	}
}
