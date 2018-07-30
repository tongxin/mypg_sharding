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

/* for internal use only. */
static const char *pg_bin_path = NULL;

static void
init_pg_bin_path()
{
	SPI_connect();
	// get pg_dump path
	if (SPI_execute("select setting from pg_config where name = 'BINDIR';", true, 0) < 0)
	{
		elog(FATAL, "mypg_sharding: Failed to query pg_config");
	}

	pg_bin_path = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
	SPI_finish();
}

static const char*
get_pg_bin_path()
{
	if (pg_bin_path == NULL)
		init_pg_bin_path();
	return pg_bin_path;
}

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
	SPI_connect();
	if (SPI_execute(cmd, true, 0) != SPI_OK_SELECT ||
		SPI_processed != 1) // the number of returned rows is not 1
	{
		elog(ERROR, "mypg_sharding: failed to retrieve connection info for node %s", nodename);
	}
	pfree(cmd);
	host = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
	port = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);
	db = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3);

	SPI_finish();
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
		n_cmds += 1;
		pfree(connstr);

		if (PQstatus(con) != CONNECTION_OK)
		{
			chan[n_cmds-1].err = psprintf("Failed to connect to node %s: %s", node,
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

		elog(INFO, "Sending command '%s' to %s", fin_sql.data, node);
		if (!send_query(con, &chan[n_cmds], fin_sql.data) ||
			(sequential && !collect_result(con, &chan[n_cmds])))
		{
			goto cleanup;
		}
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
		if (chan[i].res)
		{
			appendStringInfo(&resp, i == 0 ? "%s" : ", %s",
						 	/* chan[i].node, */ chan[i].res);
			pfree(chan[i].res);
		}
		if (chan[i].err)
		{
			appendStringInfo(&errstr, i == 0 ? "%s:%s" : ", %s:%s",
						 	chan[i].node, chan[i].err);
			pfree(chan[i].err);
		}
		PQfinish(con);
	}

	pfree(chan);

	text *res_msg;
	text *res_err;
	int res_sz;
	int err_sz;
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

	res_sz = VARHDRSZ+strlen(resp.data);
	err_sz = VARHDRSZ+strlen(errstr.data);
	res_msg = (text*)palloc(res_sz);
	res_err = (text*)palloc(err_sz);
	SET_VARSIZE(res_msg, res_sz);
	SET_VARSIZE(res_err, err_sz);
	strcpy(VARDATA(res_msg), resp.data);
	strcpy(VARDATA(res_err), errstr.data);
	datums[0] = PointerGetDatum(res_msg);
	datums[1] = PointerGetDatum(res_err);
	isnull[0] = strlen(resp.data) ? false : true;
	isnull[1] = strlen(errstr.data) ? false : true;

	PG_RETURN_DATUM(HeapTupleGetDatum(
				heap_form_tuple(tupdesc, datums, isnull)));
}

#define COPYBUFSIZ 8192

PG_FUNCTION_INFO_V1(copy_table_data);
Datum
copy_table_data(PG_FUNCTION_ARGS)
{
	char *relname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char *node = text_to_cstring(PG_GETARG_TEXT_PP(1));
	
	char *err_msg = NULL;

	char pg_dump_path[MAXPGPATH];
	char *pg_dump_cmd;
	char *this_db;
	char *this_port;
	char *this_user;

	// get pg_dump path
	join_path_components(pg_dump_path, get_pg_bin_path(), "pg_dump");
	canonicalize_path(pg_dump_path);
	// prepare the pg_dump command
	SPI_connect();
	SPI_execute("select current_database()", true, 0);
	this_db = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

	SPI_execute("select setting from pg_settings where name = 'port'", true, 0);
	this_port = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

	SPI_execute("select current_user", true, 0);
	this_user = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

	SPI_finish();

	pg_dump_cmd = psprintf("%s -t %s -a -d %s -U %s -p %s",
						   pg_dump_path, relname, this_db, this_user, this_port);
	pfree(relname);
	pfree(this_db);
	pfree(this_port);
	pfree(this_user);

	/*
	 * Wire the copy command and data outputted by pg_dump to the remote server
	 */
	FILE *s;
	char buf[COPYBUFSIZ];
	bool done = false;
	int lineno = 0;
	volatile PQExpBuffer query_buf;
	volatile PQExpBuffer linebuf;
	int query_pos = 0;
	bool in_copy = false;

	char *connstr;
	PGconn *conn;
	PGresult *res;

	if ((s = popen(pg_dump_cmd, "r")) == NULL)
	{
		err_msg = psprintf("mypg_sharding: pg_dump command failed -- %s", pg_dump_cmd);
		goto return_result;
	}

	if ((connstr = query_node_constr(node)) == NULL)
	{
		err_msg = psprintf("mypg_sharding: Failed to find connection string for node %s", node);
		fclose(s);
		goto return_result;
	}

	conn = PQconnectdb(connstr);
	pfree(connstr);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		err_msg = psprintf("mypg_sharding: Failed to connect to node %s: %s", node, PQerrorMessage(conn));
		PQfinish(conn);
		fclose(s);
		goto return_result;
	}
	pfree(node);

	query_buf = createPQExpBuffer();
	linebuf = createPQExpBuffer();
	
	while (!done)
	{	
		int  linelen;
		bool in_comment = false;
		bool command_or_copy_done = false;

		resetPQExpBuffer(query_buf);
		query_pos = 0;

		while (!command_or_copy_done)
		{
			/*
			 * Running within this loop if the current command or data copying is not completed yet.
			 */
			char *fgetres;
			char *comment;

			// Assuming copy end marker '\.' is followed by a newline
			fgetres = fgets(buf, sizeof(buf), s);
			
			if (!fgetres) 
			{
				done = true;
				break;
			}
		
			if (in_comment)
			{
				if (buf[strlen(buf) - 1] == '\n')
					in_comment = false;
				goto end_of_line;
			}

			appendPQExpBufferStr(linebuf, buf);
			if (PQExpBufferBroken(linebuf))
			{
				elog(PANIC, "Out of memory.");
			}
			linelen = linebuf->len;

			if (linebuf->data[linelen - 1] != '\n')
			{
				/*
				 * Current line is not finshed yet, continue to read.
				 */
				continue;
			}

			if (!in_copy)
			{
				linebuf->data[linelen - 1] = '\0';
			}

			if (++lineno == 1 && strncmp(buf, "PGDMP", 5) == 0)
			{
				elog(ERROR, "Copy dumped data: custom-format pg_dump is not supported.");
			}
				
			/*
			 * Read and process a line
			 */

			if (in_copy)
			{
				int copy_ret = 1;
				
				if (strcmp(linebuf->data, "\\.\n") == 0 ||
					(copy_ret = PQputCopyData(conn, linebuf->data, linelen)) <= 0)
				{
					/*
					 * End data copying due to either an encounter of EOF or some other error.
					 */
					while (PQresultStatus(res = PQgetResult(conn)) == PGRES_COPY_IN)
					{
						PQclear(res);
						PQputCopyEnd(conn, copy_ret > 0 ? NULL : "aborted because of read failure");
					}
					if (PQresultStatus(res) != PGRES_COMMAND_OK)
					{
						elog(ERROR, "mypg_sharding: %s", PQerrorMessage(conn));
					}
					in_copy = false;
					command_or_copy_done = true;
				}

				goto end_of_line;
			}
			
			comment = strstr(linebuf->data, "--");
			if (comment)
			{
				in_comment = true;
				*comment = '\0';
			}
			
			appendPQExpBufferStr(query_buf, linebuf->data);

			if (!strchr(linebuf->data, ';'))
			{
				/*
				 * The end of line has not concluded a command. Continue to read next line. 
				 */
				goto end_of_line;
			}

			/*
			 * Now that an entire command has bean read we are read to send it.
			 */
//			elog(INFO, "%s", query_buf->data+query_pos); 
			res = PQexec(conn, query_buf->data+query_pos);
			switch (PQresultStatus(res))
			{
			case PGRES_COPY_IN:
				in_copy = true;
				break;
			case PGRES_EMPTY_QUERY:
			case PGRES_COMMAND_OK:
			case PGRES_TUPLES_OK:
			{
				/*
				 * Non-copy commands executed successfully. Next move forward. 
				 */
				break;
			}
			default:
				elog(ERROR, "mypg_sharding: PQexec error: %s", PQerrorMessage(conn));
			}
			PQclear(res);
			query_pos = query_buf->len;
			command_or_copy_done = true;

		end_of_line:
			resetPQExpBuffer(linebuf);
		}
	}
	destroyPQExpBuffer(query_buf);
	destroyPQExpBuffer(linebuf);
	PQfinish(conn);
	fclose(s);

	text *res_msg;
	text *res_err;
	bool  isnull[2];
	Datum datums[2];
	TupleDesc tupdesc;

return_result:
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
	{
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning record called in context that cannot accept type record")));
	}
	BlessTupleDesc(tupdesc);

	res_msg = err_msg ? cstring_to_text("FAIL") : cstring_to_text("SUCCESS");
	res_err = err_msg ? cstring_to_text(err_msg) : cstring_to_text("");
	datums[0] = PointerGetDatum(res_msg);
	datums[1] = PointerGetDatum(res_err);
	isnull[0] = false;
	isnull[1] = err_msg ? false : true;

	HeapTuple tuple = heap_form_tuple(tupdesc, datums, isnull);
	Datum datum = HeapTupleGetDatum(tuple);
	PG_RETURN_DATUM(datum);
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
	join_path_components(pg_dump_path, get_pg_bin_path(), "pg_dump");
	canonicalize_path(pg_dump_path);
	// get dbname
	SPI_execute("select current_database()", true, 0);
	dbname = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
	// get port
	SPI_execute("select setting from pg_settings where name = 'port'", true, 0);
	port = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
	// get user
	SPI_execute("select current_user", true, 0);
	user = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
	SPI_finish();

	pg_dump_cmd = psprintf("%s -t %s -a -d %s -U %s -p %s",
						   pg_dump_path, table_name, dbname, user, port);
	pfree(dbname);
	pfree(port);
	pfree(user);

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
pq_conninfo_parse(PG_FUNCTION_ARGS)
{
	TupleDesc            tupdesc;
	/* array of keywords and array of vals as in PQconninfoOption */
	Datum		values[2];
	bool		nulls[2] = { false, false };
	ArrayType *keywords; /* array of keywords */
	ArrayType *vals; /* array of vals */
	text **keywords_txt; /* we construct array of keywords from it */
	text **vals_txt; /* array of vals constructed from it */
	Datum *elems; /* just to convert text * to it */
	int32 text_size;
	int numopts = 0;
	int i;
	size_t len;
	int16 typlen;
	bool typbyval;
	char typalign;
	char *pqerrmsg;
	char *errmsg_palloc;
	char *conninfo = text_to_cstring(PG_GETARG_TEXT_PP(0));
	PQconninfoOption *opts = PQconninfoParse(conninfo, &pqerrmsg);
	PQconninfoOption *opt;
	HeapTuple res_heap_tuple;

	if (pqerrmsg != NULL)
	{
		/* free malloced memory to avoid leakage */
		errmsg_palloc = pstrdup(pqerrmsg);
		PQfreemem((void *) pqerrmsg);
		elog(ERROR, "SHARDMAN: PQconninfoParse failed: %s", errmsg_palloc);
	}

	/* compute number of opts and allocate text ptrs */
	for (opt = opts; opt->keyword != NULL; opt++)
	{
		/* We are interested only in filled values */
		if (opt->val != NULL)
			numopts++;
	}
	keywords_txt = palloc(numopts * sizeof(text*));
	vals_txt = palloc(numopts * sizeof(text*));

	/* Fill keywords and vals */
	for (opt = opts, i = 0; opt->keyword != NULL; opt++)
	{
		if (opt->val != NULL)
		{
			len = strlen(opt->keyword);
			text_size = VARHDRSZ + len;
			keywords_txt[i] = (text *) palloc(text_size);
			SET_VARSIZE(keywords_txt[i], text_size);
			memcpy(VARDATA(keywords_txt[i]), opt->keyword, len);

			len = strlen(opt->val);
			text_size = VARHDRSZ + len;
			vals_txt[i] = (text *) palloc(text_size);
			SET_VARSIZE(vals_txt[i], text_size);
			memcpy(VARDATA(vals_txt[i]), opt->val, len);
			i++;
		}
	}

	/* Now construct arrays */
	elems = (Datum*) palloc(numopts * sizeof(Datum));
	/* get info about text type, we will pass it to array constructor */
	get_typlenbyvalalign(TEXTOID, &typlen, &typbyval, &typalign);

	/* cast text * to datums for purity and construct array */
	for (i = 0; i < numopts; i++) {
		elems[i] = PointerGetDatum(keywords_txt[i]);
	}
	keywords = construct_array(elems, numopts, TEXTOID, typlen, typbyval,
							   typalign);
	/* same for valus */
	for (i = 0; i < numopts; i++) {
		elems[i] = PointerGetDatum(vals_txt[i]);
	}
	vals = construct_array(elems, numopts, TEXTOID, typlen, typbyval,
							   typalign);

	/* prepare to form the tuple */
	values[0] = PointerGetDatum(keywords);
	values[1] = PointerGetDatum(vals);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));
	BlessTupleDesc(tupdesc); /* Inshallah */

	PQconninfoFree(opts);
	res_heap_tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(res_heap_tuple));
}


Datum
get_system_id(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(GetSystemIdentifier());
}
