/*
 * [y] hybris Platform
 *
 * Copyright (c) 2018 SAP SE or an SAP affiliate company. All rights reserved.
 *
 * This software is the confidential and proprietary information of SAP
 * ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the
 * license agreement you entered into with SAP.
 */
package de.hybris.platform.task.impl;


import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.common.collect.Lists;


public abstract class DefaultTaskServiceNodeGroupsIntegrationTest extends DefaultTaskServiceBaseTest
{

	private CountDownLatch countDownLatch;
	private CountDownLatch deletionCountDownLatch;

	@Test
	public void shouldExecuteOnlyTasksForOwnNodeGroup() throws Exception
	{
		assureTaskEngineStopped();

		final String group_a = "group_a";
		final String group_b = "group_b";

		final Long tA1 = createTask(group_a);
		final Long tA2 = createTask(group_a);
		final Long tA3 = createTask(group_a);

		final Long tB1 = createTask(group_b);
		final Long tB2 = createTask(group_b);
		final Long tB3 = createTask(group_b);
		final Long tB4 = createTask(group_b);

		final Long tNone1 = createTask(null);
		final Long tNone2 = createTask(null);
		final Long tNone3 = createTask(null);

		final Collection<Long> relevantTasks = Lists.newArrayList(tA1, tA2, tA3, tB1, tB2, tB3, tB4, tNone1, tNone2, tNone3);

		countDownLatch = new CountDownLatch(relevantTasks.size());
		deletionCountDownLatch = new CountDownLatch(relevantTasks.size());

		final TestTaskService serviceA = new TestTaskService(0, Lists.newArrayList(group_a), countDownLatch, deletionCountDownLatch, relevantTasks);
		final TestTaskService serviceB = new TestTaskService(1, Lists.newArrayList(group_b), countDownLatch, deletionCountDownLatch, relevantTasks);

		try
		{
			serviceB.getEngine().start();
			serviceA.getEngine().start();

			assertThat(countDownLatch.await(40, TimeUnit.SECONDS)).isTrue();

			assertThat(serviceA.getExecutedTasks()).contains(tA1, tA2, tA3).doesNotContain(tB1, tB2, tB3, tB4);
			assertThat(serviceB.getExecutedTasks()).contains(tB1, tB2, tB3, tB4).doesNotContain(tA1, tA2, tA3);

			final List<Long> allExecutedTasks = Lists.newArrayList(serviceA.getExecutedTasks());
			allExecutedTasks.addAll(serviceB.getExecutedTasks());

			assertThat(allExecutedTasks).containsOnlyOnce(relevantTasks.toArray(new Long[relevantTasks.size()]));

			assertThat(deletionCountDownLatch.await(40, TimeUnit.SECONDS)).isTrue();
		}
		finally
		{
			serviceB.getEngine().stop();
			serviceA.getEngine().stop();
		}
	}

	@Test
	public void shouldExecuteOnlyTasksForOwnNodeGroupInExclusiveMode() throws Exception
	{
		assureTaskEngineStopped();
		enableExclusiveMode();

		final String group_a = "group_a";
		final String group_b = "group_b";

		final Long tA1 = createTask(group_a);
		final Long tA2 = createTask(group_a);
		final Long tA3 = createTask(group_a);

		final Long tB1 = createTask(group_b);
		final Long tB2 = createTask(group_b);
		final Long tB3 = createTask(group_b);
		final Long tB4 = createTask(group_b);

		final Long tNone1 = createTask(null);
		final Long tNone2 = createTask(null);
		final Long tNone3 = createTask(null);

		final Collection<Long> relevantTasks = Lists.newArrayList(tA1, tA2, tA3, tB1, tB2, tB3, tB4, tNone1, tNone2, tNone3);

		countDownLatch = new CountDownLatch(relevantTasks.size() - 3);
		deletionCountDownLatch = new CountDownLatch(relevantTasks.size() - 3);

		final TestTaskService serviceA = new TestTaskService(0, Lists.newArrayList(group_a), countDownLatch, deletionCountDownLatch, relevantTasks);
		final TestTaskService serviceB = new TestTaskService(1, Lists.newArrayList(group_b), countDownLatch, deletionCountDownLatch, relevantTasks);


		try
		{
			serviceB.getEngine().start();
			serviceA.getEngine().start();

			assertThat(countDownLatch.await(40, TimeUnit.SECONDS)).isTrue();

			assertThat(serviceA.getExecutedTasks()).containsOnly(tA1, tA2, tA3);
			assertThat(serviceB.getExecutedTasks()).containsOnly(tB1, tB2, tB3, tB4);

			assertThat(deletionCountDownLatch.await(40, TimeUnit.SECONDS)).isTrue();
		}
		finally
		{
			serviceB.getEngine().stop();
			serviceA.getEngine().stop();
		}
	}

	@Test
	public void shouldFailAllExpiredTasks() throws Exception
	{
		assureTaskEngineStopped();

		final String group_a = "group_a";
		final String group_b = "group_b";

		//expired tasks
		final Long tA1ex = createExpiredTask(group_a);
		final Long tB1ex = createExpiredTask(group_b);
		final Long tNone1ex = createExpiredTask(null);

		final Collection<Long> relevantTasks = Lists.newArrayList(tA1ex, tB1ex, tNone1ex);

		countDownLatch = new CountDownLatch(relevantTasks.size());
		deletionCountDownLatch = new CountDownLatch(relevantTasks.size());

		final TestTaskService serviceA = new TestTaskService(0, Lists.newArrayList(group_a), countDownLatch, deletionCountDownLatch, relevantTasks);
		final TestTaskService serviceB = new TestTaskService(1, Lists.newArrayList(group_b), countDownLatch, deletionCountDownLatch, relevantTasks);

		try
		{
			serviceB.getEngine().start();
			serviceA.getEngine().start();

			assertThat(countDownLatch.await(40, TimeUnit.SECONDS)).isTrue();

			assertThat(serviceA.getExecutedTasks()).isEmpty();
			assertThat(serviceB.getExecutedTasks()).isEmpty();

			final List<Long> failedTasks = Lists.newArrayList(serviceA.getFailedTasks());
			failedTasks.addAll(serviceB.getFailedTasks());
			assertThat(failedTasks).containsOnlyOnce(relevantTasks.toArray(new Long[relevantTasks.size()]));

			assertThat(deletionCountDownLatch.await(40, TimeUnit.SECONDS)).isTrue();
		}
		finally
		{
			serviceB.getEngine().stop();
			serviceA.getEngine().stop();
		}
	}


	@Test
	public void shouldFailOnlyExpiredTasksForOwnNodeGroupInExclusiveMode() throws Exception
	{
		assureTaskEngineStopped();
		enableExclusiveMode();

		final String group_a = "group_a";
		final String group_b = "group_b";

		//expired tasks
		final Long tA1ex = createExpiredTask(group_a);
		final Long tB1ex = createExpiredTask(group_b);
		final Long tNone1ex = createExpiredTask(null);

		final Collection<Long> relevantTasks = Lists.newArrayList(tA1ex, tB1ex, tNone1ex);

		countDownLatch = new CountDownLatch(relevantTasks.size() - 1);
		deletionCountDownLatch = new CountDownLatch(relevantTasks.size() - 1);

		final TestTaskService serviceA = new TestTaskService(0, Lists.newArrayList(group_a), countDownLatch, deletionCountDownLatch, relevantTasks);
		final TestTaskService serviceB = new TestTaskService(1, Lists.newArrayList(group_b), countDownLatch, deletionCountDownLatch, relevantTasks);

		try
		{
			serviceB.getEngine().start();
			serviceA.getEngine().start();

			assertThat(countDownLatch.await(40, TimeUnit.SECONDS)).isTrue();

			assertThat(serviceA.getExecutedTasks()).isEmpty();
			assertThat(serviceB.getExecutedTasks()).isEmpty();

			assertThat(serviceA.getFailedTasks()).contains(tA1ex);
			assertThat(serviceB.getFailedTasks()).contains(tB1ex);

			assertThat(deletionCountDownLatch.await(40, TimeUnit.SECONDS)).isTrue();
		}
		finally
		{
			serviceB.getEngine().stop();
			serviceA.getEngine().stop();
		}
	}


	@Test
	public void shouldExecuteOnlyTasksForOwnNode() throws Exception
	{
		assureTaskEngineStopped();

		final String group_a = "group_a";
		final String group_b = "group_b";

		final Long tA1 = createTask(0);
		final Long tA2 = createTask(0);
		final Long tA3 = createTask(0);

		final Long tB1 = createTask(1);
		final Long tB2 = createTask(1);
		final Long tB3 = createTask(1);
		final Long tB4 = createTask(1);

		final Long tNone1 = createTask(null);
		final Long tNone2 = createTask(null);
		final Long tNone3 = createTask(null);

		final Collection<Long> relevantTasks = Lists.newArrayList(tA1, tA2, tA3, tB1, tB2, tB3, tB4, tNone1, tNone2, tNone3);

		countDownLatch = new CountDownLatch(relevantTasks.size());
		deletionCountDownLatch = new CountDownLatch(relevantTasks.size());

		final TestTaskService serviceA = new TestTaskService(0, Lists.newArrayList(group_a), countDownLatch, deletionCountDownLatch, relevantTasks);
		final TestTaskService serviceB = new TestTaskService(1, Lists.newArrayList(group_b), countDownLatch, deletionCountDownLatch, relevantTasks);

		try
		{
			serviceB.getEngine().start();
			serviceA.getEngine().start();

			assertThat(countDownLatch.await(40, TimeUnit.SECONDS)).isTrue();

			assertThat(serviceA.getExecutedTasks()).contains(tA1, tA2, tA3).doesNotContain(tB1, tB2, tB3, tB4);
			assertThat(serviceB.getExecutedTasks()).contains(tB1, tB2, tB3, tB4).doesNotContain(tA1, tA2, tA3);

			final List<Long> allExecutedTasks = Lists.newArrayList(serviceA.getExecutedTasks());
			allExecutedTasks.addAll(serviceB.getExecutedTasks());

			assertThat(allExecutedTasks).containsOnlyOnce(relevantTasks.toArray(new Long[relevantTasks.size()]));

			assertThat(deletionCountDownLatch.await(40, TimeUnit.SECONDS)).isTrue();
		}
		finally
		{
			serviceB.getEngine().stop();
			serviceA.getEngine().stop();
		}
	}

	@Test
	public void shouldFailOnlyExpiredTasksForOwnNodeIdInExclusiveMode() throws Exception
	{
		assureTaskEngineStopped();
		enableExclusiveMode();

		final String group_a = "group_a";
		final String group_b = "group_b";

		//expired tasks
		final Long tA1ex = createExpiredTask(0);
		final Long tB1ex = createExpiredTask(1);
		final Long tNone1ex = createExpiredTask(null);

		final Collection<Long> relevantTasks = Lists.newArrayList(tA1ex, tB1ex, tNone1ex);

		countDownLatch = new CountDownLatch(relevantTasks.size() - 1);
		deletionCountDownLatch = new CountDownLatch(relevantTasks.size() - 1);

		final TestTaskService serviceA = new TestTaskService(0, Lists.newArrayList(group_a), countDownLatch, deletionCountDownLatch, relevantTasks);
		final TestTaskService serviceB = new TestTaskService(1, Lists.newArrayList(group_b), countDownLatch, deletionCountDownLatch, relevantTasks);

		try
		{
			serviceB.getEngine().start();
			serviceA.getEngine().start();

			assertThat(countDownLatch.await(40, TimeUnit.SECONDS)).isTrue();

			assertThat(serviceA.getExecutedTasks()).isEmpty();
			assertThat(serviceB.getExecutedTasks()).isEmpty();

			assertThat(serviceA.getFailedTasks()).contains(tA1ex).doesNotContain(tB1ex);
			assertThat(serviceB.getFailedTasks()).contains(tB1ex).doesNotContain(tA1ex);

			assertThat(deletionCountDownLatch.await(40, TimeUnit.SECONDS)).isTrue();
		}
		finally
		{
			serviceB.getEngine().stop();
			serviceA.getEngine().stop();
		}
	}


	@Test
	public void shouldExecuteOnlyTasksForOwnNodeWhenNoGroupDefined() throws Exception
	{
		assureTaskEngineStopped();

		final Long tA1 = createTask(0);
		final Long tA2 = createTask(0);
		final Long tA3 = createTask(0);

		final Long tB1 = createTask(1);
		final Long tB2 = createTask(1);
		final Long tB3 = createTask(1);
		final Long tB4 = createTask(1);

		final Long tNone1 = createTask(null);
		final Long tNone2 = createTask(null);
		final Long tNone3 = createTask(null);

		final Collection<Long> relevantTasks = Lists.newArrayList(tA1, tA2, tA3, tB1, tB2, tB3, tB4, tNone1, tNone2, tNone3);

		countDownLatch = new CountDownLatch(relevantTasks.size());
		deletionCountDownLatch = new CountDownLatch(relevantTasks.size());

		final TestTaskService serviceA = new TestTaskService(0, Collections.emptyList(), countDownLatch, deletionCountDownLatch, relevantTasks);
		final TestTaskService serviceB = new TestTaskService(1, Collections.emptyList(), countDownLatch, deletionCountDownLatch, relevantTasks);

		try
		{
			serviceB.getEngine().start();
			serviceA.getEngine().start();

			assertThat(countDownLatch.await(40, TimeUnit.SECONDS)).isTrue();

			assertThat(serviceA.getExecutedTasks()).contains(tA1, tA2, tA3).doesNotContain(tB1, tB2, tB3, tB4);
			assertThat(serviceB.getExecutedTasks()).contains(tB1, tB2, tB3, tB4).doesNotContain(tA1, tA2, tA3);

			final List<Long> allExecutedTasks = Lists.newArrayList(serviceA.getExecutedTasks());
			allExecutedTasks.addAll(serviceB.getExecutedTasks());

			assertThat(allExecutedTasks).containsOnlyOnce(relevantTasks.toArray(new Long[relevantTasks.size()]));

			assertThat(deletionCountDownLatch.await(40, TimeUnit.SECONDS)).isTrue();
		}
		finally
		{
			serviceB.getEngine().stop();
			serviceA.getEngine().stop();
		}
	}


	@Test
	public void shouldExecuteOnlyTasksForOwnNodeInExclusiveMode() throws Exception
	{
		assureTaskEngineStopped();
		enableExclusiveMode();

		final String group_a = "group_a";
		final String group_b = "group_b";

		final Long tA1 = createTask(0);
		final Long tA2 = createTask(0);
		final Long tA3 = createTask(0);

		final Long tB1 = createTask(1);
		final Long tB2 = createTask(1);
		final Long tB3 = createTask(1);
		final Long tB4 = createTask(1);

		final Long tNone1 = createTask(null);
		final Long tNone2 = createTask(null);
		final Long tNone3 = createTask(null);

		final Collection<Long> relevantTasks = Lists.newArrayList(tA1, tA2, tA3, tB1, tB2, tB3, tB4, tNone1, tNone2, tNone3);

		countDownLatch = new CountDownLatch(relevantTasks.size() - 3);
		deletionCountDownLatch = new CountDownLatch(relevantTasks.size() - 3);

		final TestTaskService serviceA = new TestTaskService(0, Lists.newArrayList(group_a), countDownLatch, deletionCountDownLatch, relevantTasks);
		final TestTaskService serviceB = new TestTaskService(1, Lists.newArrayList(group_b), countDownLatch, deletionCountDownLatch, relevantTasks);

		try
		{
			serviceB.getEngine().start();
			serviceA.getEngine().start();

			assertThat(countDownLatch.await(40, TimeUnit.SECONDS)).isTrue();

			assertThat(serviceA.getExecutedTasks()).containsOnly(tA1, tA2, tA3);
			assertThat(serviceB.getExecutedTasks()).containsOnly(tB1, tB2, tB3, tB4);

			assertThat(deletionCountDownLatch.await(40, TimeUnit.SECONDS)).isTrue();
		}
		finally
		{
			serviceB.getEngine().stop();
			serviceA.getEngine().stop();
		}
	}

	@Test
	public void shouldExecuteTasksWithConflictingNodeIfAndNodeGroupAssignment() throws Exception
	{
		assureTaskEngineStopped();

		final String group_a = "group_a";
		final String group_b = "group_b";

		final Long tA1 = createTask(0, group_b);
		final Long tA2 = createTask(0, group_b);
		final Long tA3 = createTask(0, group_b);

		final Long tB1 = createTask(1, group_a);
		final Long tB2 = createTask(1, group_a);
		final Long tB3 = createTask(1, group_a);
		final Long tB4 = createTask(1, group_a);

		final Long tNone1 = createTask(null);
		final Long tNone2 = createTask(null);
		final Long tNone3 = createTask(null);

		final Collection<Long> relevantTasks = Lists.newArrayList(tA1, tA2, tA3, tB1, tB2, tB3, tB4, tNone1, tNone2, tNone3);

		countDownLatch = new CountDownLatch(relevantTasks.size());
		deletionCountDownLatch = new CountDownLatch(relevantTasks.size());

		final TestTaskService serviceA = new TestTaskService(0, Lists.newArrayList(group_a), countDownLatch, deletionCountDownLatch, relevantTasks);
		final TestTaskService serviceB = new TestTaskService(1, Lists.newArrayList(group_b), countDownLatch, deletionCountDownLatch, relevantTasks);

		try
		{
			serviceB.getEngine().start();
			serviceA.getEngine().start();

			assertThat(countDownLatch.await(40, TimeUnit.SECONDS)).isTrue();

			assertThat(serviceA.getExecutedTasks()).contains(tA1, tA2, tA3).doesNotContain(tB1, tB2, tB3, tB4);
			assertThat(serviceB.getExecutedTasks()).contains(tB1, tB2, tB3, tB4).doesNotContain(tA1, tA2, tA3);

			final List<Long> allExecutedTasks = Lists.newArrayList(serviceA.getExecutedTasks());
			allExecutedTasks.addAll(serviceB.getExecutedTasks());

			assertThat(allExecutedTasks).containsOnlyOnce(relevantTasks.toArray(new Long[relevantTasks.size()]));

			assertThat(deletionCountDownLatch.await(40, TimeUnit.SECONDS)).isTrue();
		}
		finally
		{
			serviceB.getEngine().stop();
			serviceA.getEngine().stop();
		}
	}


	@Test
	public void shouldExecuteTaskWithoutSpecifiedNodeGroupByAnyNode() throws Exception
	{
		assureTaskEngineStopped();

		final String group_a = "group_a";
		final String group_b = "group_b";

		final Long t1 = createTask(null);
		final Long t2 = createTask(null);
		final Long t3 = createTask(null);
		final Long t4 = createTask(null);

		final Collection<Long> relevantTasks = Lists.newArrayList(t1, t2, t3, t4);

		countDownLatch = new CountDownLatch(relevantTasks.size());
		deletionCountDownLatch = new CountDownLatch(relevantTasks.size());

		final TestTaskService serviceA = new TestTaskService(0, Lists.newArrayList(group_a), countDownLatch, deletionCountDownLatch, relevantTasks);
		final TestTaskService serviceB = new TestTaskService(1, Lists.newArrayList(group_b), countDownLatch, deletionCountDownLatch, relevantTasks);

		try
		{
			serviceB.getEngine().start();
			serviceA.getEngine().start();

			assertThat(countDownLatch.await(40, TimeUnit.SECONDS)).isTrue();

			final Collection<Long> allExecutedTasks = new ArrayList<>();

			allExecutedTasks.addAll(serviceA.getExecutedTasks());
			allExecutedTasks.addAll(serviceB.getExecutedTasks());

			assertThat(allExecutedTasks).hasSize(4);
			assertThat(allExecutedTasks).containsOnly(t1, t2, t3, t4);

			assertThat(deletionCountDownLatch.await(40, TimeUnit.SECONDS)).isTrue();
		}
		finally
		{
			serviceB.getEngine().stop();
			serviceA.getEngine().stop();
		}
	}
}
