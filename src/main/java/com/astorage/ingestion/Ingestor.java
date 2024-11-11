package com.astorage.ingestion;

import com.astorage.db.RocksDBRepository;
import io.vertx.core.Vertx;
import io.vertx.ext.web.RoutingContext;

public abstract class Ingestor {
	protected final RoutingContext context;
	protected final RocksDBRepository dbRep;
	protected final RocksDBRepository universalVariantDbRep;
	protected final RocksDBRepository fastaDbRep;
	protected final Vertx vertx;

	public Ingestor(
		RoutingContext context,
		RocksDBRepository dbRep,
		RocksDBRepository universalVariantDbRep,
		RocksDBRepository fastaDbRep,
		Vertx vertx
	) {
		this.context = context;
		this.dbRep = dbRep;
		this.universalVariantDbRep = universalVariantDbRep;
		this.fastaDbRep = fastaDbRep;
		this.vertx = vertx;
	}

	@SuppressWarnings("unused")
	public abstract void ingestionHandler();
}
