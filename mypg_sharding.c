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
PG_FUNCTION_INFO_V1(broadcast);
PG_FUNCTION_INFO_V1(reconstruct_table_attrs);
PG_FUNCTION_INFO_V1(get_system_id);

/* GUC variables */
static bool _is_master;
static char *_node_name;
static char *nodestate;

extern void _PG_init(void);

/*
 * Entrypoint of the module. Define GUCs.
 */
void
_PG_init()
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
	text *out = (text *) palloc(varsz);
	SET_VARSIZE(out, varsz);
	memcpy(VARDATA(out), _node_name, sz);
	PG_RETURN_TEXT_P(out);
}

static bool
wait_for_response(PGconn *conn)
{
	if (!conn)
		return false;

	while (conn->asyncStatus == PGASYNC_BUSY) {
		int res;
		/* flush what's unsent in the buffer
		 */
		while ((res = pqFlush(conn)) > 0)
		{
			if (pqWait(false, true, conn))
			{
				res = -1;
				break;
			}
		}
		
		if (res ||							// res < 0,  pgFlush failure  
			pqWait(true, false, conn) ||    // failure in waiting for read
			pqReadData(conn) < 0) 			// failure in reading data
		{
			/*
			 * conn->errorMessage has been set by pqWait or pqReadData. We
			 * want to append it to any already-received error message.
			 */
			pqSaveErrorResult(conn);
			conn->asyncStatus = PGASYNC_IDLE;
			return false;
		}
	}

	return true;
}

typedef struct
{
	PGconn* con;
	char*   node;
	char*   sql;
} Channel;

#define MAX_NODENAME_SZ 256

/* 
 * Returns connection string if the node name is matched in mypg.nodes
 * otherwise returns NULL. 
 * Make sure SPI_connect() is already called within the context.  
 */
static char*
check_and_get_node_constr(const char *nodename)
{
	char *cmd;
	char *host;
	char *port;
	char *db;

	cmd = psprintf("select host, port, dbname from mypg.nodes where node_name = %s", nodename);
	if (SPI_execute(cmd, true, 0) != SPI_OK_SELECT ||
		SPI_processed != 1)         // the number of returned rows is not 1
	{
		elog(ERROR, "mypg_sharding: failed to retrieve connection info for node %d", nodename);
	}
	pfree(cmd);
	host = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3);
	port = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4);
	db   = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 5);

	/* turns host and port into a connection string. */
	return psprintf("host=%s port=%s dbname=%s", host, port, db);
}

Datum
broadcast(PG_FUNCTION_ARGS)
{
	char* sql_full = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char* cmd = pstrdup(sql_full);
	bool  two_phase = PG_GETARG_BOOL(1);
	bool  sequential = PG_GETARG_BOOL(2);
	char* iso_level = (PG_GETARG_POINTER(3) != NULL) ?
		text_to_cstring(PG_GETARG_TEXT_PP(3)) : NULL;
	char* sep;
	char* sql;
	char* colon;
	PGresult *res;
	char *conninfo_cmd;
	int   rc;
	int	  n;
	char* node;
	char* connstr;
	int   n_cmds = 0;
	int   i;
	int n_cons = 1024; /* num of channels allocated currently */
	Channel* chan;
	PGconn* con;
	StringInfoData resp;
	StringInfoData fin_sql;

	char const* errstr = "";

	elog(DEBUG1, "Broadcast commmand '%s'",  cmd);

	initStringInfo(&resp);

	SPI_connect();
	chan = (Channel*) palloc(sizeof(Channel) * n_cons);

	/* Open connections and send all queries */
	while ((sep = strchr(cmd, *cmd == '{' ? '}' : ';')) != NULL)
	{
		*sep = '\0';

		if (*cmd == '{')
		{
			cmd += 1;
		}
		
		/* Extract node name from the next command then construct a connection string. */
		if (colon = strchr(cmd, ':'))
		{
			*colon = '\0';
			node = cmd;
		}
		if (colon == NULL ||
			(connstr = check_and_get_node_constr(node)) == NULL)
		{
			elog(ERROR, "mypg_sharding: invalid broadcast command: '%s' in '%s'.",
				 cmd, sql_full);
		}

		sql = colon + 1;  // starting 1-off the colon
		cmd = sep + 1;

		if (n_cmds >= n_cons)
		{
			chan = (Channel*) repalloc(chan, sizeof(Channel) * (n_cons *= 2));
		}

		/* Set up connection to the target node. */
		con = PQconnectdb(connstr);
		chan[n_cmds].con = con;
		chan[n_cmds].node = node;
		chan[n_cmds].sql = sql;
		n_cmds += 1;

		pfree(connstr);

		if (PQstatus(con) != CONNECTION_OK)
		{
			errstr = psprintf("Failed to connect to node %s: %s", node,
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
		if (!PQsendQuery(con, fin_sql.data)
			|| (sequential && !wait_for_response(con)))
		{
			errstr = psprintf("Failed to send query '%s' to node %s: %s'", fin_sql.data,
							  node, PQerrorMessage(con));
			goto cleanup;
		}
	}

	if (*cmd != '\0')
	{
		elog(ERROR, "mypg_sharding: Junk at end of command list: %s in %s", cmd, sql_full);
	}

	/*
	 * Now collect results
	 */
	for (i = 0; i < n_cmds; i++)
	{
		PGresult* next_res;
		PGresult* res = NULL;
		ExecStatusType status;

		con = chan[i].con;

		/* Skip all but the last result */
		while ((next_res = PQgetResult(con)) != NULL)
		{
			if (res != NULL)
			{
				PQclear(res);
			}
			res = next_res;
		}

		if (res == NULL)
		{
			errstr = psprintf("Failed to receive response for query %s from node %s: %s",
							  chan[i].sql, chan[i].node, PQerrorMessage(con));
			goto cleanup;
		}

		/* Result was successfully fetched, add it to resp */
		status = PQresultStatus(res);
		if (status != PGRES_EMPTY_QUERY && 
			status != PGRES_TUPLES_OK   &&
			status != PGRES_COMMAND_OK)
		{
			errstr = psprintf("Command %s failed at node %s: %s",
							  chan[i].sql, chan[i].node, PQerrorMessage(con));
			PQclear(res);
			goto cleanup;
		}
		if (i != 0)
		{
			appendStringInfoChar(&resp, ',');
		}
		/* When rows are actually returned */
		if (status == PGRES_TUPLES_OK)
		{
			if (PQntuples(res) != 1 || PQgetisnull(res, 0, 0))
			{
				errstr = psprintf("Query '%s' doesn't return single tuple at node %s",
								  chan[i].sql, chan[i].node);
				PQclear(res);
				goto cleanup;
			}
			else
			{
				appendStringInfo(&resp, "%s", PQgetvalue(res, 0, 0));
			}
		}
		else
		{
			appendStringInfo(&resp, "%d", PQntuples(res));
		}
		PQclear(res);
	}

  cleanup:
	for (i = 0; i < n_cmds; i++)
	{
		con = chan[i].con;
		if (two_phase)
		{
			if (*errstr)
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
		PQfinish(con);
	}

	if (*errstr)
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
				 errmsg("mypg_sharding: %s", errstr)));
	}

	pfree(chan);
	SPI_finish();

	PG_RETURN_TEXT_P(cstring_to_text(resp.data));
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
	text *sql = (text*) palloc(pallocated);
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
	join_path_component(pg_dump_path, 
						SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1),
						"pg_dump");
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
			sql = (text *) repalloc(sql, pallocated);
		}
		/* since we realloc, can't just += bytes_read here */
		ptr = VARDATA(sql) + VARSIZE_ANY_EXHDR(sql);
	}

	if (pclose(fd))	{
		elog(ERROR, "SHARDING: pg_dump exited with error status, output was\n%scmd was \n%s",
			 text_to_cstring(sql), pg_dump_cmd);
	}

	pfree(pg_dump_cmd);
	PG_RETURN_TEXT_P(sql);
}
/*
 * Generate CREATE TABLE sql for relation via pg_dump. We use it for root
 * (parent) tables because pg_dump dumps all the info -- indexes, constrains,
 * defaults, everything. Parameter is not REGCLASS because pg_dump can't
 * handle oids anyway. Connstring must be proper libpq connstring, it is feed
 * to pg_dump.
 * TODO: actually we should have muchmore control on what is dumped, so we
 * need to copy-paste parts of messy pg_dump or collect the needed data
 * manually walking over catalogs.
 */
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
	text *sql = (text *) palloc(pallocated);
	char *ptr = VARDATA(sql); /* ptr to first free byte */
	char *cmd;
	FILE *fp;
	size_t bytes_read;

	SET_VARSIZE(sql, VARHDRSZ);

	/* find pg_dump location querying pg_config */
	SPI_connect();
	if (SPI_execute("select setting from pg_config where name = 'BINDIR';",
					true, 0) < 0)
		elog(FATAL, "SHARDING: Failed to query pg_config");
	strcpy(pg_dump_path,
		   SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
	SPI_finish();
	join_path_components(pg_dump_path, pg_dump_path, "pg_dump");
	canonicalize_path(pg_dump_path);

	cmd = psprintf("%s -t '%s' --no-owner --schema-only --dbname='%s' 2>&1",
				   pg_dump_path, relation, shardlord_connstring);

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
			sql = (text *) repalloc(sql, pallocated);
		}
		/* since we realloc, can't just += bytes_read here */
		ptr = VARDATA(sql) + VARSIZE_ANY_EXHDR(sql);
	}

	if (pclose(fp))	{
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
	Oid	relid = PG_GETARG_OID(0);
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
						 (attr->attcollation ?
						  psprintf(" COLLATE \"%s\"",
								   get_collation_name(attr->attcollation)) :
						  ""));
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

