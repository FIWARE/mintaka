package org.fiware.mintaka.domain;

public enum TimeRelation {

    BEFORE("before"),
    AFTER("after"),
    BETWEEN("between");

    private final String value;

    private TimeRelation(java.lang.String value) {
        this.value = value;
    }

}
