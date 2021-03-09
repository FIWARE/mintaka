package org.fiware.mintaka.persistence;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor
@Getter
public class LimitableResult<T> {

	private final T result;
	private final boolean limited;
}
