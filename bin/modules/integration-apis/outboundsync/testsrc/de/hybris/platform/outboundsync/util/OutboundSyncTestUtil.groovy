/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */

package de.hybris.platform.outboundsync.util


import de.hybris.platform.core.model.product.ProductModel
import de.hybris.platform.cronjob.model.CronJobModel
import de.hybris.platform.impex.jalo.ImpExException
import de.hybris.platform.integrationservices.model.IntegrationObjectModel
import de.hybris.platform.integrationservices.util.IntegrationTestUtil
import de.hybris.platform.outboundsync.model.OutboundChannelConfigurationModel

import static de.hybris.platform.integrationservices.util.IntegrationTestUtil.importCatalogVersion
import static de.hybris.platform.integrationservices.util.IntegrationTestUtil.importImpEx
import static de.hybris.platform.outboundservices.ConsumedDestinationBuilder.consumedDestinationBuilder
import static de.hybris.platform.outboundsync.OutboundChannelConfigurationBuilder.outboundChannelConfigurationBuilder

class OutboundSyncTestUtil {

	private static final String DEFAULT_CATALOG_VERSION = "Default:Staged"

	static OutboundChannelConfigurationModel outboundChannelConfigurationExists(final String channelCode, final String integrationObjectCode) {
		outboundChannelConfigurationBuilder()
				.withCode(channelCode)
				.withIntegrationObjectCode(integrationObjectCode)
				.withConsumedDestination(consumedDestinationBuilder())
				.build()
	}

	static ProductModel importProductWithCode(final String code)
	{
		importCatalogVersion("Staged", "Default", true)
		importImpEx(
				"INSERT_UPDATE Product; code[unique = true]; catalogVersion(catalog(id), version)",
				"                     ; $code              ; $DEFAULT_CATALOG_VERSION"
		)
		getProductByCode(code)
	}

	static void outboundSyncRetryExists(final Long itemPk, final String channelConfigurationCode) throws ImpExException {
		importImpEx(
				"INSERT_UPDATE OutboundSyncRetry; itemPk[unique = true]; syncAttempts; channel(code)",
				"                               ; $itemPk              ; 3           ; $channelConfigurationCode ")
	}

	static ProductModel getProductByCode(final String code)
	{
		return IntegrationTestUtil.findAny(ProductModel, { it.code == code }).orElse(null)
	}

	static OutboundChannelConfigurationModel getChannelConfigurationByAttributes(String code, IntegrationObjectModel integrationObject) {

		return IntegrationTestUtil.findAny(OutboundChannelConfigurationModel, { it.code == code && it.integrationObject == integrationObject }).orElse(null)
	}

	static CronJobModel outboundCronJob() {
		return IntegrationTestUtil.findAny(CronJobModel, { it.code == "outboundSyncCronJob"}).orElse(null)
	}
}
