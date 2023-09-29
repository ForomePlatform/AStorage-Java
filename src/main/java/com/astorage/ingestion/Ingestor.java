package com.astorage.ingestion;

import com.astorage.db.RocksDBRepository;
import io.vertx.ext.web.RoutingContext;

public abstract class Ingestor {
	protected final RoutingContext context;
	protected final RocksDBRepository dbRep;
	protected final RocksDBRepository universalVariantDbRep;
	protected final RocksDBRepository fastaDbRep;

	public Ingestor(
		RoutingContext context,
		RocksDBRepository dbRep,
		RocksDBRepository universalVariantDbRep,
		RocksDBRepository fastaDbRep
	) {
		this.context = context;
		this.dbRep = dbRep;
		this.universalVariantDbRep = universalVariantDbRep;
		this.fastaDbRep = fastaDbRep;
	}

	@SuppressWarnings("unused")
	public abstract void ingestionHandler();
}
