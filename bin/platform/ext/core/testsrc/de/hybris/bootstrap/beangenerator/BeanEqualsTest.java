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
package de.hybris.bootstrap.beangenerator;

import de.hybris.bootstrap.annotations.UnitTest;
import de.hybris.platform.test.beans.TestBean;

import org.junit.Test;

import static org.junit.Assert.*;

@UnitTest
public class BeanEqualsTest
{

	@Test
	public void testEquals()
	{
		final TestBean bean = new TestBean();
		bean.setEqualsA("string");
		bean.setEqualsB(Integer.valueOf(1234));
		bean.setEqualsC(Boolean.TRUE);

		final TestBean bean2 = new TestBean();
		bean2.setEqualsA("string");
		bean2.setEqualsB(Integer.valueOf(1234));
		bean2.setEqualsC(Boolean.TRUE);

		assertEquals(bean, bean);
		assertEquals(bean, bean2);

		bean2.setEqualsA("different");
		assertEquals(bean, bean);
		assertNotEquals(bean, bean2);
	}

	@Test
	public void testEqualsCornerCases()
	{
		final TestBean bean = new TestBean();
		// null
		assertNotEquals(bean, null);
		// other type
		assertNotEquals(bean, new Integer(1234));
	}
}
