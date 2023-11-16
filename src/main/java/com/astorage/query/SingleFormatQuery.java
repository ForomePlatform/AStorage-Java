package com.astorage.query;

import com.astorage.db.RocksDBRepository;
import io.vertx.ext.web.RoutingContext;

public abstract class SingleFormatQuery implements Query {
	protected final RoutingContext context;
	protected final RocksDBRepository dbRep;

	public SingleFormatQuery(RoutingContext context, RocksDBRepository dbRep) {
		this.context = context;
		this.dbRep = dbRep;
	}

	@SuppressWarnings("unused")
	public static String[] normalizedParamsToParams(
		String refBuild,
		String chr,
		String pos,
		String ref,
		String alt
	) {
		return null;
	}
}
