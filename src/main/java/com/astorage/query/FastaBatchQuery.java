package com.astorage.query;

import com.astorage.db.RocksDBRepository;
import com.astorage.utils.Constants;
import com.astorage.utils.fasta.FastaConstants;
import io.vertx.ext.web.RoutingContext;

@SuppressWarnings("unused")
public class FastaBatchQuery extends FastaQuery implements Query, Constants, FastaConstants {
	public FastaBatchQuery(RoutingContext context, RocksDBRepository dbRep) {
		super(context, dbRep);
	}

	public void queryHandler() {
	}
}
