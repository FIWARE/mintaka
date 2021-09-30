package org.fiware.mintaka.persistence;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigInteger;
import java.time.LocalDateTime;

@Data
@AllArgsConstructor
public class AggregatedAttribute {

	private LocalDateTime timeBucket;

	private BigInteger count;
	private BigInteger distinctCount;
	private Double avg;
	private Double sum;
	private Double stddev;
	private Double sumsq;
	private String min;
	private String max;

}
