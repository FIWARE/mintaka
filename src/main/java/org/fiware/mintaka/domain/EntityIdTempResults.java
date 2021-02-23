package org.fiware.mintaka.domain;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.net.URI;
import java.time.Instant;

@Data
@RequiredArgsConstructor
public class EntityIdTempResults {

	private final String entiyId;
	private final Instant startTime;
	private final Instant endTime;

}
