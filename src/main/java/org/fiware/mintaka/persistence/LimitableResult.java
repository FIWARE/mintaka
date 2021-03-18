package org.fiware.mintaka.persistence;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class LimitableResult<T> {

	private final T result;
	private final boolean limited;
}
