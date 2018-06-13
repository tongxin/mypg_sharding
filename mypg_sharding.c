/* -------------------------------------------------------------------------
 *
 * mypg_sharding.c
 *
 * Copyright (c) 2018, Tongxin Bai
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/pg_type.h"
#include "commands/event_trigger.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/latch.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

/* ensure that extension won't load against incompatible version of Postgres */
PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(is_master);
PG_FUNCTION_INFO_V1(node_name);
PG_FUNCTION_INFO_V1(broadcast);
PG_FUNCTION_INFO_V1(reconstruct_table_attrs);
PG_FUNCTION_INFO_V1(get_system_id);

/* GUC variables */
static bool _is_master;
static char *_node_name;

extern void _PG_init(void);

/*
 * Entrypoint of the module. Define GUCs.
 */
void _PG_init()
{
	DefineCustomBoolVariable(
		"mypg.is_master",
		"This node is the master?",
		NULL,
		&_is_master,
		false,
		PGC_SUSET,
		0,
		NULL, NULL, NULL);

	DefineCustomStringVariable(
		"mypg.node_name",
		"Node name",
		"The node name used in the sharding cluster context",
		&_node_name,
		"",
		PGC_SUSET,
		0,
		NULL, NULL, NULL);
}

Datum
	is_master(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(_is_master);
}

Datum
	node_name(PG_FUNCTION_ARGS)
{
	size_t sz = strlen(_node_name);
	size_t varsz = sz + VARHDRSZ;
	text *out = (text *)palloc(varsz);
	SET_VARSIZE(out, varsz);
	memcpy(VARDATA(out), _node_name, sz);
	PG_RETURN_TEXT_P(out);
}

// static bool
// wait_for_response(PGconn *conn)
// {
// 	if (!conn)
// 		return false;

// 	while (conn->asyncStatus == PGASYNC_BUSY) {
// 		int res;
// 		/* flush what's unsent in the buffer
// 		 */
// 		while ((res = pqFlush(conn)) > 0)
// 		{
// 			if (pqWait(false, true, conn))
// 			{
// 				res = -1;
// 				break;
// 			}
// 		}

// 		if (res ||							// res < 0,  pgFlush failure
// 			pqWait(true, false, conn) ||    // failure in waiting for read
// 			pqReadData(conn) < 0) 			// failure in reading data
// 		{
// 			/*
// 			 * conn->errorMessage has been set by pqWait or pqReadData. We
// 			 * want to append it to any already-received error message.
// 			 */
// 			pqSaveErrorResult(conn);
// 			conn->asyncStatus = PGASYNC_IDLE;
// 			return false;
// 		}
// 	}
// 	return true;
// }

typedef struct
{
	PGconn *con;
	char *node;
	char *sql;
	char *res;
	char *err;
} Channel;

static bool
send_query(PGconn *conn, Channel *chan, char *query)
{
	if (!PQsendQuery(conn, query))
	{
		chan->err = psprintf("Failed to send query '%s' to node %s: %s'", query,
							  chan->node, PQerrorMessage(conn));
		return false;
	}
	return true;
}

static bool
collect_result(PGconn *conn, Channel *chan)
{
	PGresult *res = NULL;
	PGresult *next_res;
	ExecStatusType status;

	while ((next_res = PQgetResult(conn)) != NULL)
	{
		if (res != NULL)
		{
			PQclear(res);
		}
		res = next_res;
	}

	if (res == NULL)
	{
		chan->err = psprintf("Failed to receive response for query %s from node %s: %s",
						 chan->sql, chan->node, PQerrorMessage(conn));
		return false;
	}

	/* Result was successfully fetched, add it to resp */
	status = PQresultStatus(res);
	if (status != PGRES_EMPTY_QUERY &&
		status != PGRES_TUPLES_OK &&
		status != PGRES_COMMAND_OK)
	{
		chan->err = psprintf("Command %s failed at node %s: %s",
						 chan->sql, chan->node, PQerrorMessage(conn));
		PQclear(res);
		return false;
	}

	/* When rows are actually returned */
	if (status == PGRES_TUPLES_OK)
	{
		if (PQntuples(res) != 1 || PQgetisnull(res, 0, 0))
		{
			chan->err = psprintf("Query '%s' doesn't return single tuple at node %s",
							  chan->sql, chan->node);
			PQclear(res);
			return false;
		}
		else
		{
			chan->res = pstrdup(PQgetvalue(res, 0, 0));
		}
	}
	else
	{
		chan->res = "";
	}
	PQclear(res);
	return true;
}

#define MAX_NODENAME_SZ 256

/* 
 * Returns connection string if the node name is matched in mypg.cluster_nodes
 * otherwise returns NULL. 
 * Make sure SPI_connect() is already called within the context.  
 */
static char *
query_node_constr(const char *nodename)
{
	char *cmd;
	char *host;
	char *port;
	char *db;

	cmd = psprintf("select host, port, dbname from mypg.cluster_nodes where node_name = '%s'", nodename);
	if (SPI_execute(cmd, true, 0) != SPI_OK_SELECT ||
		SPI_processed != 1) // the number of returned rows is not 1
	{
		elog(ERROR, "mypg_sharding: failed to retrieve connection info for node %s", nodename);
	}
	pfree(cmd);
	host = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3);
	port = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4);
	db = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 5);

	/* turns host and port into a connection string. */
	return psprintf("host=%s port=%s dbname=%s", host, port, db);
}

Datum
broadcast(PG_FUNCTION_ARGS)
{
	char *sql_full = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char *cmd = pstrdup(sql_full);
	bool two_phase = PG_GETARG_BOOL(1);
	bool sequential = PG_GETARG_BOOL(2);
	char *iso_level = (PG_GETARG_POINTER(3) != NULL) ? text_to_cstring(PG_GETARG_TEXT_PP(3)) : NULL;
	char *sep;
	char *sql;
	char *colon;
	PGresult *res;
	char *node;
	char *connstr;
	int n_cmds = 0;
	int i;
	int n_cons = 1024; /* num of channels allocated currently */
	Channel *chan;
	PGconn *con;
	StringInfoData resp;
	StringInfoData fin_sql;
	StringInfoData errstr;
	bool query_failed = false;

	elog(DEBUG1, "Broadcast commmand '%s'", cmd);

	SPI_connect();
	chan = (Channel *)palloc(sizeof(Channel) * n_cons);

	/* Open connections and send all queries */
	while ((sep = strchr(cmd, *cmd == '{' ? '}' : ';')) != NULL)
	{
		*sep = '\0';

		if (*cmd == '{')
		{
			cmd += 1;
		}

		/* Extract node name from the next command then construct a connection string. */
		if ((colon = strchr(cmd, ':')))
		{
			*colon = '\0';
			node = cmd;
		}
		if (colon == NULL ||
			(connstr = query_node_constr(node)) == NULL)
		{
			elog(ERROR, "mypg_sharding: invalid broadcast command: '%s' in '%s'.",
				 cmd, sql_full);
		}

		sql = colon + 1; // starting 1-off the colon
		cmd = sep + 1;

		if (n_cmds >= n_cons)
		{
			chan = (Channel *)repalloc(chan, sizeof(Channel) * (n_cons *= 2));
		}

		/* Set up connection to the target node. */
		con = PQconnectdb(connstr);
		chan[n_cmds].con = con;
		chan[n_cmds].node = node;
		chan[n_cmds].sql = sql;
		chan[n_cmds].res = NULL;
		chan[n_cmds].err = NULL;

		pfree(connstr);

		if (PQstatus(con) != CONNECTION_OK)
		{
			chan->err = psprintf("Failed to connect to node %s: %s", node,
							     PQerrorMessage(con));
			goto cleanup;
		}
		/* Build the actual sql to send, mem freed with ctxt */
		initStringInfo(&fin_sql);
		if (iso_level)
			appendStringInfo(&fin_sql, "BEGIN TRANSACTION ISOLATION LEVEL %s; ", iso_level);
		appendStringInfoString(&fin_sql, sql);
		appendStringInfoChar(&fin_sql, ';'); /* it was removed after strchr */
		if (two_phase)
			appendStringInfoString(&fin_sql, "PREPARE TRANSACTION 'shardlord';");
		else if (iso_level)
			appendStringInfoString(&fin_sql, "END;");

		elog(DEBUG1, "Sending command '%s' to node %s", fin_sql.data, node);
		if (!send_query(con, &chan[n_cmds], fin_sql.data) ||
			(sequential && !collect_result(con, &chan[n_cmds])))
		{
			goto cleanup;
		}

		n_cmds += 1;
	}

	if (*cmd != '\0')
	{
		elog(ERROR, "mypg_sharding: Junk at end of command list: %s in %s", cmd, sql_full);
	}

	if (!sequential) 
	{
		/* Get results */
		for (i = 0; i < n_cmds; i++)
		{
			collect_result(con, &chan[i]);
		}
	}

cleanup:
	initStringInfo(&resp);
	initStringInfo(&errstr);

	for (i = 0; i < n_cmds && !chan[i].err; i++);
	if (i < n_cmds)
	{
		query_failed = true;
	}
	
	for (i = 0; i < n_cmds; i++)
	{
		con = chan[i].con;
		if (two_phase)
		{
			if (query_failed)
			{
				res = PQexec(con, "ROLLBACK PREPARED 'mypg'");
				if (PQresultStatus(res) != PGRES_COMMAND_OK)
				{
					elog(WARNING, "mypg_sharding: Rollback of 2PC failed at node %s: %s",
						 chan[i].node, PQerrorMessage(con));
				}
				PQclear(res);
			}
			else
			{
				res = PQexec(con, "COMMIT PREPARED 'mypg'");
				if (PQresultStatus(res) != PGRES_COMMAND_OK)
				{
					elog(WARNING, "mypg_sharding: 2PC failed at node %s: %s",
						 chan[i].node, PQerrorMessage(con));
				}
				PQclear(res);
			}
		}
		if (chan[i].err)
		{
			appendStringInfo(&resp, i == 0 ? "%s:%s" : ", %s:%s",
						 	chan[i].node, chan[i].res);
		}
		if (chan[i].err)
		{
			appendStringInfo(&errstr, i == 0 ? "%s:%s" : ", %s:%s",
						 	chan[i].node, chan[i].err);
		}
		pfree(chan[i].res);
		pfree(chan[i].err);
		PQfinish(con);
	}

	pfree(chan);
	SPI_finish();

	TupleDesc tupdesc;
	Datum datums[2];
	bool isnull[2];

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
	{
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning record called in context that cannot accept type record")));
	}
	BlessTupleDesc(tupdesc);
	datums[0] = CStringGetDatum(resp.data);
	datums[1] = CStringGetDatum(errstr.data);
	isnull[0] = strlen(resp.data) ? false : true;
	isnull[1] = strlen(errstr.data) ? false : true;

	return HeapTupleGetDatum(
			heap_form_tuple(tupdesc, datums, isnull));
}

/*
 * Generate sql for copying table from one machine to another. 
 */
PG_FUNCTION_INFO_V1(gen_copy_table_sql);
Datum
gen_copy_table_sql(PG_FUNCTION_ARGS)
{
	char *table_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char pg_dump_path[MAXPGPATH];
	const size_t chunksize = 128;
	size_t pallocated = VARHDRSZ + chunksize;
	text *sql = (text *)palloc(pallocated);
	char *ptr = VARDATA(sql);
	char *pg_dump_cmd;
	char *dbname;
	char *user;
	char *port;
	FILE *fd;
	size_t bytes_read;

	SET_VARSIZE(sql, VARHDRSZ);

	SPI_connect();
	// get pg_dump path
	if (SPI_execute("select setting from pg_config where name = 'BINDIR';",
					true, 0) < 0)
		elog(FATAL, "mypg_sharding: Failed to query pg_config");
	join_path_components(pg_dump_path,
						 SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1),
						 "pg_dump");
	canonicalize_path(pg_dump_path);
	// get dbname
	SPI_execute("select current_database()", true, 0);
	dbname = pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
	// get port
	SPI_execute("select setting from pg_settings where name = 'port'", true, 0);
	port = pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
	// get user
	SPI_execute("select current_user", true, 0);
	user = pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
	SPI_finish();

	pg_dump_cmd = psprintf("%s -t %s -a -d %s -U %s -p %s",
						   pg_dump_path, table_name, dbname, user, port);

	if ((fd = popen(pg_dump_cmd, "r")) == NULL)
	{
		elog(ERROR, "SHARDING: Failed to run pg_dump -- %s", pg_dump_cmd);
	}

	while ((bytes_read = fread(ptr, sizeof(char), chunksize, fd)) != 0)
	{
		SET_VARSIZE(sql, VARSIZE_ANY(sql) + bytes_read);
		if (pallocated - VARSIZE_ANY(sql) < chunksize)
		{
			pallocated *= 2;
			sql = (text *)repalloc(sql, pallocated);
		}
		/* since we realloc, can't just += bytes_read here */
		ptr = VARDATA(sql) + VARSIZE_ANY_EXHDR(sql);
	}

	if (pclose(fd))
	{
		elog(ERROR, "SHARDING: pg_dump exited with error status, output was\n%scmd was \n%s",
			 text_to_cstring(sql), pg_dump_cmd);
	}

	pfree(pg_dump_cmd);
	PG_RETURN_TEXT_P(sql);
}

PG_FUNCTION_INFO_V1(gen_create_table_sql);
Datum
gen_create_table_sql(PG_FUNCTION_ARGS)
{
	char pg_dump_path[MAXPGPATH];
	/* let the mmgr free that */
	char *relation = text_to_cstring(PG_GETARG_TEXT_PP(0));
	const size_t chunksize = 5; /* read max that bytes at time */
	/* how much already allocated *including header* */
	size_t pallocated = VARHDRSZ + chunksize;
	text *sql = (text *)palloc(pallocated);
	char *ptr = VARDATA(sql); /* ptr to first free byte */
	char *cmd;
	FILE *fp;
	size_t bytes_read;

	char *dbname;
	char *port;
	char *user;

	SET_VARSIZE(sql, VARHDRSZ);

	/* find pg_dump location querying pg_config */
	SPI_connect();
	if (SPI_execute("select setting from pg_config where name = 'BINDIR';",
					true, 0) < 0)
		elog(FATAL, "SHARDING: Failed to query pg_config");
	strcpy(pg_dump_path,
		   SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));

	join_path_components(pg_dump_path, pg_dump_path, "pg_dump");
	canonicalize_path(pg_dump_path);

	// get dbname
	SPI_execute("select current_database()", true, 0);
	dbname = pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
	// get port
	SPI_execute("show port", true, 0);
	port = pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
	// get user
	SPI_execute("select current_user", true, 0);
	user = pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));

	SPI_finish();

	cmd = psprintf("%s -t '%s' --no-owner --schema-only --dbname='%s -U %s -p %s' 2>&1",
				   pg_dump_path, relation, dbname, user, port);

	if ((fp = popen(cmd, "r")) == NULL)
	{
		elog(ERROR, "SHARDING: Failed to run pg_dump, cmd %s", cmd);
	}

	while ((bytes_read = fread(ptr, sizeof(char), chunksize, fp)) != 0)
	{
		SET_VARSIZE(sql, VARSIZE_ANY(sql) + bytes_read);
		if (pallocated - VARSIZE_ANY(sql) < chunksize)
		{
			pallocated *= 2;
			sql = (text *)repalloc(sql, pallocated);
		}
		/* since we realloc, can't just += bytes_read here */
		ptr = VARDATA(sql) + VARSIZE_ANY_EXHDR(sql);
	}

	if (pclose(fp))
	{
		elog(ERROR, "SHARDING: pg_dump exited with error status, output was\n%scmd was \n%s",
			 text_to_cstring(sql), cmd);
	}

	PG_RETURN_TEXT_P(sql);
}

/*
 * Reconstruct attrs part of CREATE TABLE stmt, e.g. (i int NOT NULL, j int).
 * The only constraint reconstructed is NOT NULL.
 */
Datum
reconstruct_table_attrs(PG_FUNCTION_ARGS)
{
	StringInfoData query;
	Oid relid = PG_GETARG_OID(0);
	Relation local_rel = heap_open(relid, AccessExclusiveLock);
	TupleDesc local_descr = RelationGetDescr(local_rel);
	int i;

	initStringInfo(&query);
	appendStringInfoChar(&query, '(');

	for (i = 0; i < local_descr->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(local_descr, i);

		if (i != 0)
			appendStringInfoString(&query, ", ");

		/* NAME TYPE[(typmod)] [NOT NULL] [COLLATE "collation"] */
		appendStringInfo(&query, "%s %s%s%s",
						 quote_identifier(NameStr(attr->attname)),
						 format_type_with_typemod(attr->atttypid,
												  attr->atttypmod),
						 (attr->attnotnull ? " NOT NULL" : ""),
						 (attr->attcollation ? psprintf(" COLLATE \"%s\"",
														get_collation_name(attr->attcollation))
											 : ""));
	}

	appendStringInfoChar(&query, ')');

	/* Let xact unlock this */
	heap_close(local_rel, NoLock);
	PG_RETURN_TEXT_P(cstring_to_text(query.data));
}

Datum
get_system_id(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(GetSystemIdentifier());
}
