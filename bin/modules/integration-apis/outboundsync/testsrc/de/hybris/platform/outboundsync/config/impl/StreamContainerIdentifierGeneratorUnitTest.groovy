/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */
package de.hybris.platform.outboundsync.config.impl

import de.hybris.platform.outboundsync.model.OutboundChannelConfigurationModel
import org.junit.Test
import spock.lang.Specification

class StreamContainerIdentifierGeneratorUnitTest extends Specification {

	def identifierGenerator = new StreamContainerIdentifierGenerator()

	@Test
	def "generate"() {
		given:
		def channelCode = "testChannelCode"
		def channel = Stub(OutboundChannelConfigurationModel)
				{
					getCode() >> channelCode
				}

		expect:
		identifierGenerator.generate(channel) == channelCode + "Container"
	}
}
