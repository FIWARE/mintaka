package org.fiware.mintaka.domain;

/**
 * Internal representation of a timerelation
 */
public enum TimeRelation {

    BEFORE("before"),
    AFTER("after"),
    BETWEEN("between");

    private final String value;

    TimeRelation(java.lang.String value) {
        this.value = value;
    }

}
