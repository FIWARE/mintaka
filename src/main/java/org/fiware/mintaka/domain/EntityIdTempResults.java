package org.fiware.mintaka.domain;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.net.URI;
import java.time.Instant;

/**
 * Pojo to hold temporary query results with the start and endtime a query hit for the given entity.
 */
@Data
@RequiredArgsConstructor
public class EntityIdTempResults {

	private final String entityId;
	private final Instant startTime;
	private final Instant endTime;

}
