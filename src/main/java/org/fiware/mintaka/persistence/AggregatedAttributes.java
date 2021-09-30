package org.fiware.mintaka.persistence;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.List;

@Data
@RequiredArgsConstructor
public class AggregatedAttributes {

	private final String entityId;
	private final String attributeId;
	private final List<AggregatedAttribute> aggregatedAttributes;
}
