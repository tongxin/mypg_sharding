/*-------------------------------------------------------------------------
 *
 * postgres_fdw.h
 *		  Foreign-data wrapper for remote PostgreSQL servers
 *
 * Portions Copyright (c) 2012-2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/postgres_fdw/postgres_fdw.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MYPG_FDW_H
#define MYPG_FDW_H

#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/relation.h"
#include "utils/relcache.h"

#include "libpq-fe.h"

/*
 * FDW-specific planner information kept in RelOptInfo.fdw_private for a
 * mypg_fdw foreign table.  For a baserel, this struct is created by
 * postgresGetForeignRelSize, although some fields are not filled till later.
 */
typedef struct RelationInfo
{
	/*
	 * True means that the relation can be pushed down. Always true for simple
	 * foreign scan.
	 */
	bool pushdown_safe;

	/*
	 * The distributed table is sharded on this column.
	 */  
	AttrNumber dist_keyatt;
	/*
	 * Restriction clauses are separated into shard and global. Local conditions 
	 * are not allowed. Join clauses should not be present for the current design.
	 */
	List *shard_conds;
	List *global_conds;
	List *local_conds;
	
	/* Actual remote restriction clauses for scan (sans RestrictInfos) */
	List	   *final_remote_exprs;

	/* Bitmap of attr numbers we need to fetch from the remote server. */
	Bitmapset *attrs_used;

	// /* Cost and selectivity of local_conds. */
	// QualCost	local_conds_cost;
	// Selectivity local_conds_sel;

	// /* Selectivity of join conditions */
	// Selectivity joinclause_sel;

	/* Estimated size and cost for a scan or join. */
	double rows;
	int width;
	Cost startup_cost;
	Cost total_cost;
	/* Costs excluding costs for transferring data from the foreign server */
	Cost rel_startup_cost;
	Cost rel_total_cost;

	/* Options extracted from catalogs. */
	// bool		use_remote_estimate;
	Cost fdw_startup_cost;
	Cost fdw_tuple_cost;

	/* Cached catalog information. */
	ForeignTable *table;
	ForeignServer *server;
	UserMapping *user; /* only set in use_remote_estimate mode */

	int fetch_size; /* fetch size for this remote table */

	/*
	 * Name of the relation while EXPLAINing ForeignScan. It is used for join
	 * relations but is set for all relations. For join relation, the name
	 * indicates which foreign tables are being joined and the join type used.
	 */
	StringInfo relation_name;

	/*
     * Holds local shard relation info which we'll use for cost estimation in
	 * query planning.
     */
	RelOptInfo *shardrel;

	// /* Join information */
	// RelOptInfo *outerrel;
	// RelOptInfo *innerrel;
	// JoinType	jointype;
	// /* joinclauses contains only JOIN/ON conditions for an outer join */
	// List	   *joinclauses;	/* List of RestrictInfo */

	/* Grouping information */
	List *grouped_tlist;

	// /* Subquery information */
	// bool		make_outerrel_subquery; /* do we deparse outerrel as a
	// 									 * subquery? */
	// bool		make_innerrel_subquery; /* do we deparse innerrel as a
	// 									 * subquery? */
	// Relids		lower_subquery_rels;	/* all relids appearing in lower
	// 									 * subqueries */

	// /*
	//  * Index of the relation.  It is used to create an alias to a subquery
	//  * representing the relation.
	//  */
	// int			relation_index;
} RelationInfo;


/* in mypg_connection.c */
extern PGconn *GetConnections(UserMapping *user, bool will_prep_stmt);
extern void ReleaseConnection(PGconn *conn);
extern unsigned int GetCursorNumber(PGconn *conn);
extern unsigned int GetPrepStmtNumber(PGconn *conn);
extern PGresult *pgfdw_get_result(PGconn *conn, const char *query);
extern PGresult *pgfdw_exec_query(PGconn *conn, const char *query);
extern void pgfdw_report_error(int elevel, PGresult *res, PGconn *conn,
				   bool clear, const char *sql);


/* in deparse.c */
extern void classifyConditions(PlannerInfo *root,
							AttrNumber distkey_att,
							RelOptInfo *baserel,
							List *input_conds,
							List **shard_conds,
							List **global_conds,
							List **local_conds);
extern bool is_foreign_expr(PlannerInfo *root,
							RelOptInfo *baserel,
							Expr *expr);
// extern void deparseInsertSql(StringInfo buf, RangeTblEntry *rte,
// 				 Index rtindex, Relation rel,
// 				 List *targetAttrs, bool doNothing,
// 				 List *withCheckOptionList, List *returningList,
// 				 List **retrieved_attrs);
// extern void deparseUpdateSql(StringInfo buf, RangeTblEntry *rte,
// 				 Index rtindex, Relation rel,
// 				 List *targetAttrs,
// 				 List *withCheckOptionList, List *returningList,
// 				 List **retrieved_attrs);
// extern void deparseDirectUpdateSql(StringInfo buf, PlannerInfo *root,
// 					   Index rtindex, Relation rel,
// 					   RelOptInfo *foreignrel,
// 					   List *targetlist,
// 					   List *targetAttrs,
// 					   List *remote_conds,
// 					   List **params_list,
// 					   List *returningList,
// 					   List **retrieved_attrs);
// extern void deparseDeleteSql(StringInfo buf, RangeTblEntry *rte,
// 				 Index rtindex, Relation rel,
// 				 List *returningList,
// 				 List **retrieved_attrs);
// extern void deparseDirectDeleteSql(StringInfo buf, PlannerInfo *root,
// 					   Index rtindex, Relation rel,
// 					   RelOptInfo *foreignrel,
// 					   List *remote_conds,
// 					   List **params_list,
// 					   List *returningList,
// 					   List **retrieved_attrs);
// extern void deparseAnalyzeSizeSql(StringInfo buf, Relation rel);
// extern void deparseAnalyzeSql(StringInfo buf, Relation rel,
// 				  List **retrieved_attrs);
// extern void deparseStringLiteral(StringInfo buf, const char *val);
// extern Expr *find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel);
// extern List *build_tlist_to_deparse(RelOptInfo *foreignrel);
extern void deparseSimpleSelectStmt(StringInfo buf, PlannerInfo *root, RelOptInfo *relopt,
						List *remote_conds, List *pathkeys,
						List **retrieved_attrs, List **params_list)


#endif							/* POSTGRES_FDW_H */
