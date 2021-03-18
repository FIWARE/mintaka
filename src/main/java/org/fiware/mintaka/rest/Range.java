package org.fiware.mintaka.rest;

import lombok.RequiredArgsConstructor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Pojo for holding the range information used in the http content range header.
 */
@RequiredArgsConstructor
public class Range {

	private final Instant start;
	private final Instant end;

	public LocalDateTime getStart() {
		return LocalDateTime.ofInstant(start, ZoneOffset.UTC);
	}

	public LocalDateTime getEnd() {
		return LocalDateTime.ofInstant(end, ZoneOffset.UTC);
	}
}
