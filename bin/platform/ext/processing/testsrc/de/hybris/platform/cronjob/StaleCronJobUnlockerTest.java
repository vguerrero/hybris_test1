/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */

package de.hybris.platform.cronjob;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import de.hybris.bootstrap.annotations.IntegrationTest;
import de.hybris.platform.cluster.ClusterNodeInfo;
import de.hybris.platform.cluster.DefaultClusterNodeManagementService;
import de.hybris.platform.core.Registry;
import de.hybris.platform.core.Tenant;
import de.hybris.platform.servicelayer.ServicelayerBaseTest;
import de.hybris.platform.testframework.PropertyConfigSwitcher;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@IntegrationTest
public class StaleCronJobUnlockerTest extends ServicelayerBaseTest
{

	private final PropertyConfigSwitcher propertyActive = new PropertyConfigSwitcher(
			StaleCronJobUnlocker.PROPERTY_CRONJOB_UNLOCKER_ACTIVE);
	private final PropertyConfigSwitcher propertyInterval = new PropertyConfigSwitcher(
			StaleCronJobUnlocker.PROPERTY_CRONJOB_UNLOCKER_INTERVAL);
	private final PropertyConfigSwitcher propertyNodeTimeout = new PropertyConfigSwitcher(
			StaleCronJobUnlocker.PROPERTY_CRONJOB_STALE_NODE_INTERVAL);
	private final PropertyConfigSwitcher propertyNodeTimeoutCutoffInterval = new PropertyConfigSwitcher(
			StaleCronJobUnlocker.PROPERTY_CRONJOB_STALE_NODE_CUTOFF_INTERVAL);


	private final Duration defaultThreadInterval = Duration.ofSeconds(2);
	private StaleCronJobUnlocker thread;

	@Before
	public void setUp() throws Exception
	{
		propertyActive.switchToValue("false");
		propertyInterval.switchToValue("1000");
		propertyNodeTimeout.switchToValue("2000");
		propertyNodeTimeoutCutoffInterval.switchToValue("100");
	}

	@After
	public void tearDown() throws Exception
	{
		propertyActive.switchBackToDefault();
		propertyInterval.switchBackToDefault();
		propertyNodeTimeout.switchBackToDefault();
		propertyNodeTimeoutCutoffInterval.switchBackToDefault();

		if (thread != null && thread.isAlive())
		{
			thread.interrupt();
		}
	}

	@Test
	public void shouldBeActiveWhenPropertyIsTrue() throws InterruptedException
	{
		thread = spy(new TestStaleCronJobUnlocker(Registry.getCurrentTenant()));

		propertyActive.switchToValue("true");

		thread.start();
		Thread.sleep(defaultThreadInterval.toMillis());
		thread.stopUpdatingAndFinish(defaultThreadInterval.toMillis());

		verify(thread, atLeastOnce()).getStaleNodeIds(any());
	}

	@Test
	public void shouldBeInactiveWhenPropertyIsFalse() throws InterruptedException
	{
		thread = spy(new TestStaleCronJobUnlocker(Registry.getCurrentTenant()));

		propertyActive.switchToValue("false");

		thread.start();
		Thread.sleep(defaultThreadInterval.toMillis());
		thread.stopUpdatingAndFinish(defaultThreadInterval.toMillis());

		verify(thread, never()).getStaleNodeIds(any());
	}

	@Test
	public void shouldUseProvidedInterval() throws InterruptedException
	{
		final ExecutionIntervalMarker marker = new ExecutionIntervalMarker();
		thread = spy(new TestStaleCronJobUnlocker(Registry.getCurrentTenant(), clusterNodeInfoPredicate -> {
			marker.mark();
			return Collections.emptyList();
		}));

		propertyActive.switchToValue("true");
		propertyInterval.switchToValue("1000");

		thread.start();
		Thread.sleep(defaultThreadInterval.multipliedBy(2).toMillis());

		propertyInterval.switchToValue("1500");
		Thread.sleep(defaultThreadInterval.multipliedBy(2).toMillis());

		thread.stopUpdatingAndFinish(defaultThreadInterval.toMillis());

		verify(thread, atLeastOnce()).getStaleNodeIds(any());

		final List<Duration> intervals = marker.getIntervals();
		assertThat(intervals).isNotEmpty();

		final Map<Long, Long> collect = intervals.stream()
		                                         .collect(Collectors.groupingBy(
				                                         duration -> Math.round(duration.toMillis() / 500.0) * 500,
				                                         Collectors.counting()));
		assertThat(collect.keySet()).containsExactlyInAnyOrder(1000L, 1500L);
	}

	@Test
	public void shouldReturnValidStaleNodeThresholdInterval()
	{
		thread = spy(new TestStaleCronJobUnlocker(Registry.getCurrentTenant()));

		assertThat(thread.getStaleNodeThresholdInterval()).isEqualByComparingTo(Duration.ofMillis(2000));

		propertyNodeTimeout.switchToValue("5000");
		assertThat(thread.getStaleNodeThresholdInterval()).isEqualByComparingTo(Duration.ofMillis(5000));

		propertyNodeTimeout.switchToValue("100");
		assertThat(thread.getStaleNodeThresholdInterval()).isEqualByComparingTo(Duration.ofMillis(100));

		propertyNodeTimeout.switchToValue("0");
		assertThat(thread.getStaleNodeThresholdInterval()).isEqualByComparingTo(Duration.ofMillis(
				DefaultClusterNodeManagementService.getInstance().getStaleNodeTimeout()));

		propertyNodeTimeout.switchToValue("-100");
		assertThat(thread.getStaleNodeThresholdInterval()).isEqualByComparingTo(Duration.ofMillis(
				DefaultClusterNodeManagementService.getInstance().getStaleNodeTimeout()));
	}

	@Test
	public void shouldCalculateValidStaleNodeThreshold()
	{
		thread = spy(new TestStaleCronJobUnlocker(Registry.getCurrentTenant()));

		final Duration staleNodeThresholdInterval = thread.getStaleNodeThresholdInterval();
		assertThat(staleNodeThresholdInterval).isEqualByComparingTo(Duration.ofMillis(2000));

		final Instant now = Instant.now();

		final Instant staleNodeTsThreshold = thread.getStaleNodeTsThreshold(now, staleNodeThresholdInterval);

		assertThat(staleNodeTsThreshold).isEqualByComparingTo(now.minus(staleNodeThresholdInterval));
	}


	@Test
	public void shouldCalculateValidStaleNodeCutoffWhenValueIsGreaterThanZero()
	{
		thread = spy(new TestStaleCronJobUnlocker(Registry.getCurrentTenant()));

		propertyNodeTimeoutCutoffInterval.switchToValue("20000");

		final Instant now = Instant.now();

		final Instant staleNodeTsCutoff = thread.getStaleNodeTsCutoff(now);
		assertThat(staleNodeTsCutoff).isLessThan(now);

		final Instant expectedCutoffTs = now.minus(20000, ChronoUnit.SECONDS);
		assertThat(staleNodeTsCutoff).isEqualByComparingTo(expectedCutoffTs);
	}

	@Test
	public void shouldCalculateValidStaleNodeCutoffWhenValueIsZero()
	{
		thread = spy(new TestStaleCronJobUnlocker(Registry.getCurrentTenant()));

		propertyNodeTimeoutCutoffInterval.switchToValue("0");

		final Instant now = Instant.now();

		final Instant staleNodeTsCutoff = thread.getStaleNodeTsCutoff(now);

		assertThat(staleNodeTsCutoff).isEqualByComparingTo(Instant.EPOCH);
	}

	@Test
	public void shouldCalculateValidStaleNodeCutoffWhenValueIsLesserThanZero()
	{
		thread = spy(new TestStaleCronJobUnlocker(Registry.getCurrentTenant()));

		propertyNodeTimeoutCutoffInterval.switchToValue("-1");

		final Instant now = Instant.now();

		final Instant staleNodeTsCutoff = thread.getStaleNodeTsCutoff(now);

		assertThat(staleNodeTsCutoff).isEqualByComparingTo(Instant.EPOCH);
	}


	public static class TestStaleCronJobUnlocker extends StaleCronJobUnlocker
	{

		private final Function<Predicate<ClusterNodeInfo>, List<Integer>> getStaleNodeIdsFunction;

		TestStaleCronJobUnlocker(final Tenant tenant)
		{
			this(tenant, clusterNodePredicate -> Collections.emptyList());
		}

		public TestStaleCronJobUnlocker(final Tenant tenant,
		                                final Function<Predicate<ClusterNodeInfo>, List<Integer>> getStaleNodeIdsFunction)
		{
			super("testing", tenant);
			this.getStaleNodeIdsFunction = getStaleNodeIdsFunction;
		}

		@Override
		protected List<Integer> getStaleNodeIds(final Predicate<ClusterNodeInfo> staleNodePredicate)
		{
			return getStaleNodeIdsFunction.apply(staleNodePredicate);
		}

		@Override
		protected void unlockCronJobsForNodeIds(final List<Integer> staleNodes)
		{

		}
	}

	private static class ExecutionIntervalMarker
	{

		List<Instant> executionInstants = new ArrayList<>();
		List<Duration> intervals = new ArrayList<>();
		AtomicReference<Instant> lastExecution = new AtomicReference<>();

		public void mark()
		{
			final Instant now = Instant.now();
			final Instant prev = lastExecution.getAndSet(now);

			executionInstants.add(now);
			if (prev != null)
			{
				intervals.add(Duration.between(prev, now));
			}
		}

		public List<Duration> getIntervals()
		{
			return intervals;
		}
	}

}
