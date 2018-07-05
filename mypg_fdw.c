/* -------------------------------------------------------------------------
 *
 * mypg_fdw.c
 *   
 *  mypg_sharding logic adapted in a foreign data wrapper.
 *  A considerable code reuse is based off postgres_fdw.
 *
 * Copyright (c) 2018, Tongxin Bai
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "mypg_config.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/indexing.h"
#include "catalog/pg_class.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_extension_d.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/relation.h"
#include "optimizer/cost.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/sampling.h"
#include "utils/selfuncs.h"

PG_MODULE_MAGIC;

/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST 100.0

/* Default CPU cost to process 1 row (above and beyond cpu_tuple_cost). */
#define DEFAULT_FDW_TUPLE_COST 0.01

/* If no remote estimates, assume a sort costs 20% extra */
#define DEFAULT_FDW_SORT_MULTIPLIER 1.2

/*
 * Indexes of FDW-private information stored in fdw_private lists.
 */
enum FdwScanPrivateIndex
{
	/* Select SQL statement to execute */
	FdwScanPrivateSelectSql,
	/* Integer list of attribute numbers retrieved by the SELECT */
	FdwScanPrivateRetrievedAttrs,
	/* Integer representing the desired fetch_size */
	FdwScanPrivateFetchSize
};

/*
 * Similarly, this enum describes what's kept in the fdw_private list for
 * a ModifyTable node referencing a postgres_fdw foreign table.  We store:
 *
 * 1) INSERT/UPDATE/DELETE statement text to be sent to the remote server
 * 2) Integer list of target attribute numbers for INSERT/UPDATE
 *	  (NIL for a DELETE)
 * 3) Boolean flag showing if the remote query has a RETURNING clause
 * 4) Integer list of attribute numbers retrieved by RETURNING, if any
 */
enum FdwModifyPrivateIndex
{
	/* Update SQL statement to execute */
	FdwModifyPrivateUpdateSql,
	/* Integer list of target attribute numbers for INSERT/UPDATE */
	FdwModifyPrivateTargetAttnums,
	/* has-returning flag (as an integer Value node) */
	FdwModifyPrivateHasReturning,
	/* Integer list of attribute numbers retrieved by RETURNING */
	FdwModifyPrivateRetrievedAttrs
};

/*
 * Similarly, this enum describes what's kept in the fdw_private list for
 * a ForeignScan node that modifies a foreign table directly.  We store:
 *
 * 1) UPDATE/DELETE statement text to be sent to the remote server
 * 2) Boolean flag showing if the remote query has a RETURNING clause
 * 3) Integer list of attribute numbers retrieved by RETURNING, if any
 * 4) Boolean flag showing if we set the command es_processed
 */
enum FdwDirectModifyPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwDirectModifyPrivateUpdateSql,
	/* has-returning flag (as an integer Value node) */
	FdwDirectModifyPrivateHasReturning,
	/* Integer list of attribute numbers retrieved by RETURNING */
	FdwDirectModifyPrivateRetrievedAttrs,
	/* set-processed flag (as an integer Value node) */
	FdwDirectModifyPrivateSetProcessed
};

/*
 * Execution state
 */
typedef struct PgFdwState
{
	Relation rel;			  /* relcache entry for the foreign table. NULL
								 * for a foreign join scan. */
	TupleDesc tupdesc;		  /* tuple descriptor of scan */
	AttInMetadata *attinmeta; /* attribute datatype conversion metadata */

	/* extracted fdw_private data */
	char *query;		   /* text of SELECT command */
	List *retrieved_attrs; /* list of retrieved attribute numbers */

	/* for remote query execution */

} PgFdwState;

/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(mypg_fdw_handler);

/*
 * FDW callback routines
 */
static void GetForeignRelSize(PlannerInfo *root,
							  RelOptInfo *baserel,
							  Oid foreigntableid);
static void GetForeignPaths(PlannerInfo *root,
							RelOptInfo *baserel,
							Oid foreigntableid);
static ForeignScan *GetForeignPlan(PlannerInfo *root,
								   RelOptInfo *foreignrel,
								   Oid foreigntableid,
								   ForeignPath *best_path,
								   List *tlist,
								   List *scan_clauses,
								   Plan *outer_plan);
// static void postgresBeginForeignScan(ForeignScanState *node, int eflags);
// static TupleTableSlot *postgresIterateForeignScan(ForeignScanState *node);
// static void postgresReScanForeignScan(ForeignScanState *node);
// static void postgresEndForeignScan(ForeignScanState *node);
// static void postgresAddForeignUpdateTargets(Query *parsetree,
// 											RangeTblEntry *target_rte,
// 											Relation target_relation);
// static List *postgresPlanForeignModify(PlannerInfo *root,
// 									   ModifyTable *plan,
// 									   Index resultRelation,
// 									   int subplan_index);
// static void postgresBeginForeignModify(ModifyTableState *mtstate,
// 									   ResultRelInfo *resultRelInfo,
// 									   List *fdw_private,
// 									   int subplan_index,
// 									   int eflags);
// static TupleTableSlot *postgresExecForeignInsert(EState *estate,
// 												 ResultRelInfo *resultRelInfo,
// 												 TupleTableSlot *slot,
// 												 TupleTableSlot *planSlot);
// static TupleTableSlot *postgresExecForeignUpdate(EState *estate,
// 												 ResultRelInfo *resultRelInfo,
// 												 TupleTableSlot *slot,
// 												 TupleTableSlot *planSlot);
// static TupleTableSlot *postgresExecForeignDelete(EState *estate,
// 												 ResultRelInfo *resultRelInfo,
// 												 TupleTableSlot *slot,
// 												 TupleTableSlot *planSlot);
// static void postgresEndForeignModify(EState *estate,
// 									 ResultRelInfo *resultRelInfo);
// static void postgresBeginForeignInsert(ModifyTableState *mtstate,
// 									   ResultRelInfo *resultRelInfo);
// static void postgresEndForeignInsert(EState *estate,
// 									 ResultRelInfo *resultRelInfo);
// static int postgresIsForeignRelUpdatable(Relation rel);
// static bool postgresPlanDirectModify(PlannerInfo *root,
// 									 ModifyTable *plan,
// 									 Index resultRelation,
// 									 int subplan_index);
// static void postgresBeginDirectModify(ForeignScanState *node, int eflags);
// static TupleTableSlot *postgresIterateDirectModify(ForeignScanState *node);
// static void postgresEndDirectModify(ForeignScanState *node);
// static void postgresExplainForeignScan(ForeignScanState *node,
// 									   ExplainState *es);
// static void postgresExplainForeignModify(ModifyTableState *mtstate,
// 										 ResultRelInfo *rinfo,
// 										 List *fdw_private,
// 										 int subplan_index,
// 										 ExplainState *es);
// static void postgresExplainDirectModify(ForeignScanState *node,
// 										ExplainState *es);
// static bool postgresAnalyzeForeignTable(Relation relation,
// 										AcquireSampleRowsFunc *func,
// 										BlockNumber *totalpages);
// static List *postgresImportForeignSchema(ImportForeignSchemaStmt *stmt,
// 							Oid serverOid);
// static void postgresGetForeignJoinPaths(PlannerInfo *root,
// 							RelOptInfo *joinrel,
// 							RelOptInfo *outerrel,
// 							RelOptInfo *innerrel,
// 							JoinType jointype,
// 							JoinPathExtraData *extra);
static bool postgresRecheckForeignScan(ForeignScanState *node,
									   TupleTableSlot *slot);
// static void postgresGetForeignUpperPaths(PlannerInfo *root,
// 							 UpperRelationKind stage,
// 							 RelOptInfo *input_rel,
// 							 RelOptInfo *output_rel,
// 							 void *extra);
/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
	mypg_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	/* Functions for scanning foreign tables */
	routine->GetForeignRelSize = GetForeignRelSize;
	routine->GetForeignPaths = GetForeignPaths;
	routine->GetForeignPlan = GetForeignPlan;
	routine->BeginForeignScan = postgresBeginForeignScan;
	routine->IterateForeignScan = postgresIterateForeignScan;
	routine->ReScanForeignScan = postgresReScanForeignScan;
	routine->EndForeignScan = postgresEndForeignScan;

	/* Functions for updating foreign tables */
	routine->AddForeignUpdateTargets = postgresAddForeignUpdateTargets;
	routine->PlanForeignModify = postgresPlanForeignModify;
	routine->BeginForeignModify = postgresBeginForeignModify;
	routine->ExecForeignInsert = postgresExecForeignInsert;
	routine->ExecForeignUpdate = postgresExecForeignUpdate;
	routine->ExecForeignDelete = postgresExecForeignDelete;
	routine->EndForeignModify = postgresEndForeignModify;
	routine->BeginForeignInsert = postgresBeginForeignInsert;
	routine->EndForeignInsert = postgresEndForeignInsert;
	routine->IsForeignRelUpdatable = postgresIsForeignRelUpdatable;
	routine->PlanDirectModify = postgresPlanDirectModify;
	routine->BeginDirectModify = postgresBeginDirectModify;
	routine->IterateDirectModify = postgresIterateDirectModify;
	routine->EndDirectModify = postgresEndDirectModify;

	/* Function for EvalPlanQual rechecks */
	routine->RecheckForeignScan = postgresRecheckForeignScan;
	/* Support functions for EXPLAIN */
	routine->ExplainForeignScan = postgresExplainForeignScan;
	routine->ExplainForeignModify = postgresExplainForeignModify;
	routine->ExplainDirectModify = postgresExplainDirectModify;

	/* Support functions for ANALYZE */
	routine->AnalyzeForeignTable = postgresAnalyzeForeignTable;

	/* Support functions for IMPORT FOREIGN SCHEMA */
	routine->ImportForeignSchema = NULL;

	/* Support functions for join push-down */
	routine->GetForeignJoinPaths = NULL;

	/* Support functions for upper relation push-down */
	routine->GetForeignUpperPaths = NULL;

	PG_RETURN_POINTER(routine);
}

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

static Oid mypg_extension_oid;
static Oid mypg_tables_relid;
static Oid mypg_partitions_relid;

static Oid
get_extension_schema(Oid ext_oid)
{
	Oid result;
	Relation rel;
	SysScanDesc scandesc;
	HeapTuple tuple;
	ScanKeyData entry[1];

	rel = heap_open(ExtensionRelationId, AccessShareLock);

	ScanKeyInit(&entry[0],
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ext_oid));

	scandesc = systable_beginscan(rel, ExtensionOidIndexId, true,
								  NULL, 1, entry);

	tuple = systable_getnext(scandesc);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(tuple))
		result = ((Form_pg_extension)GETSTRUCT(tuple))->extnamespace;
	else
		result = InvalidOid;

	systable_endscan(scandesc);

	heap_close(rel, AccessShareLock);

	return result;
}

void mypg_fdw_cache_init()
{
	/* initialize mypg relids */
	Oid ext_schema;

	mypg_extension_oid = get_extension_oid(MYPG_EXTENSION_NAME, true);
	ext_schema = get_extension_schema(mypg_extension_oid);
	mypg_tables_relid = get_relname_relid(MYPG_TABLES, ext_schema);
	mypg_partitions_relid = get_relname_relid(MYPG_PARTITIONS, ext_schema);
}

/*
 * Foreward declarations of deparse functions
 */
static void classifyConditions(PlannerInfo *root,
							   RelOptInfo *baserel,
							   List *input_conds,
							   List **remote_conds,
							   List **local_conds);
static bool is_foreign_expr(PlannerInfo *root,
							RelOptInfo *baserel,
							Expr *expr);

static AttrNumber
get_relation_distribution_keyatt(const char *relname)
{
	Relation 	tables;
	SysScanDesc scandesc;
	HeapTuple   tuple;
	text	   *sktext;
	ScanKeyData entry[1];
	bool		isnull;
	AttrNumber	attnum;
	Oid			ns;
	Oid			relid;
	Relation	rel;

	tables = heap_open(mypg_tables_relid, AccessShareLock);
	sktext = cstring_to_text(relname);
	ScanKeyInit(&entry[0],
				Anum_mypg_tables_relname,
				BTEqualStrategyNumber,
				F_TEXTEQ,
				PointerGetDatum(sktext));
	scandesc = systable_beginscan(tables, InvalidOid, false,
								  NULL, 1, entry);
	tuple = systable_getnext(scandesc);
	if (HeapTupleIsValid(tuple))
		attnum = heap_getattr(tuple, Anum_mypg_tables_distkey, 
							  RelationGetDescr(tables), &isnull);
	else
		elog(ERROR, "mypg_fdw: relation %s does not relate to an entry in mypy.tables.", relname);

	heap_close(mypg_tables_relid, AccessShareLock);
	return attnum;
	// ns = linitial_oid(fetch_search_path(false));
	// relid = get_relname_relid(relname, ns);
	// rel = heap_open(relid, AccessShareLock);
}

static RelOptInfo *
build_shardrel(PlannerInfo *root, RelOptInfo *baserel, const char *relname)
{
	/* Related to mypg.partitions */
	Relation parts;
	SysScanDesc scandesc;
	HeapTuple tuple;
	text *sktext;
	ScanKeyData entry[1];
	/* Related to local partition */
	Oid partid;
	RelOptInfo *partrel;

	parts = heap_open(mypg_partitions_relid, AccessShareLock);

	sktext = cstring_to_text(relname);
	ScanKeyInit(&entry[0],
				Anum_mypg_partitions_relname,
				BTEqualStrategyNumber,
				F_TEXTEQ,
				PointerGetDatum(sktext));

	scandesc = systable_beginscan(parts, InvalidOid, false,
								  NULL, 1, entry);

	tuple = systable_getnext(scandesc);
	/* Pick the first matching tuple */
	if (HeapTupleIsValid(tuple))
		partid = ((Form_mypg_partitions)GETSTRUCT(tuple))->localid;
	else
		partid = InvalidOid;

	partrel = makeNode(RelOptInfo);
	partrel->reloptkind = RELOPT_OTHER_MEMBER_REL;
	partrel->relids = bms_make_singleton(partid);
	partrel->rows = 0;
	/* cheap startup cost is interesting iff not all tuples to be retrieved */
	partrel->consider_startup = true;
	partrel->consider_param_startup = false; /* might get changed later */
	partrel->consider_parallel = false;		 /* might get changed later */
	partrel->reltarget = copy_pathtarget(baserel->reltablespace);
	partrel->pathlist = NIL;
	partrel->ppilist = NIL;
	partrel->partial_pathlist = NIL;
	partrel->cheapest_startup_path = NULL;
	partrel->cheapest_total_path = NULL;
	partrel->cheapest_unique_path = NULL;
	partrel->cheapest_parameterized_paths = NIL;
	partrel->direct_lateral_relids = NULL;
	partrel->lateral_relids = NULL;
	partrel->relid = partid;
	partrel->rtekind = RTE_RELATION;
	/* min_attr, max_attr, attr_needed, attr_widths are set below */
	partrel->lateral_vars = NIL;
	partrel->lateral_referencers = NULL;
	partrel->indexlist = NIL;
	partrel->statlist = NIL;
	partrel->pages = 0;
	partrel->tuples = 0;
	partrel->allvisfrac = 0;
	partrel->subroot = NULL;
	partrel->subplan_params = NIL;
	partrel->rel_parallel_workers = -1; /* set up in get_relation_info */
	partrel->serverid = InvalidOid;
	partrel->userid = InvalidOid;
	partrel->useridiscurrent = false;
	partrel->fdwroutine = NULL;
	partrel->fdw_private = NULL;
	partrel->unique_for_rels = NIL;
	partrel->non_unique_for_rels = NIL;
	partrel->baserestrictinfo = list_copy(baserel->baserestrictinfo);
	partrel->baserestrictcost.startup = 0;
	partrel->baserestrictcost.per_tuple = 0;
	partrel->baserestrict_min_security = UINT_MAX;
	partrel->joininfo = NIL;
	partrel->has_eclass_joins = false;
	partrel->part_scheme = NULL;
	partrel->nparts = 0;
	partrel->boundinfo = NULL;
	partrel->partition_qual = NIL;
	partrel->part_rels = NULL;
	partrel->partexprs = NULL;
	partrel->nullable_partexprs = NULL;
	partrel->partitioned_child_rels = NIL;
	partrel->top_parent_relids = NULL;
	get_relation_info(root, partid, false, partrel);

	heap_close(mypg_partitions_relid, AccessShareLock);

	return partrel;
}

/*
 * GetForeignRelSize
 *		Estimate # of rows and width of the result of the scan
 *
 * We currently only consider single base relation case. 
 */
static void
GetForeignRelSize(PlannerInfo *root,
				  RelOptInfo *baserel,
				  Oid foreigntableid)
{
	RelationInfo *fpinfo;
	ListCell *lc;
	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
	const char *namespace;
	const char *relname;
	const char *refname;

	fpinfo = (RelationInfo *)palloc0(sizeof(RelationInfo));
	baserel->fdw_private = (void *)fpinfo;

	/* Base foreign tables need to be pushed down always. */
	fpinfo->pushdown_safe = true;

	/* Look up foreign-table catalog info. */
	fpinfo->table = GetForeignTable(foreigntableid);
	fpinfo->server = GetForeignServer(fpinfo->table->serverid);

	/*
	 * Set the name of relation in fpinfo, while we are constructing it here.
	 * It will be used to build the string describing the join relation in
	 * EXPLAIN output. We can't know whether VERBOSE option is specified or
	 * not, so always schema-qualify the foreign table name.
	 */
	fpinfo->relation_name = makeStringInfo();
	namespace = get_namespace_name(get_rel_namespace(foreigntableid));
	relname = get_rel_name(foreigntableid);
	refname = rte->eref->aliasname;
	appendStringInfo(fpinfo->relation_name, "%s.%s",
					 quote_identifier(namespace),
					 quote_identifier(relname));
	if (*refname && strcmp(refname, relname) != 0)
		appendStringInfo(fpinfo->relation_name, " %s",
						 quote_identifier(rte->eref->aliasname));
	/*
	 * Extract user-settable option values.  Note that per-table setting of
	 * use_remote_estimate overrides per-server setting.
	 */
	fpinfo->fdw_startup_cost = DEFAULT_FDW_STARTUP_COST;
	fpinfo->fdw_tuple_cost = DEFAULT_FDW_TUPLE_COST;
	fpinfo->fetch_size = 100;

	/*
	 * Build distribution column for classifyConditions
	 */
	fpinfo->dist_keyatt = get_relation_distribution_keyatt(relname);
	/*
	 * Identify which baserestrictinfo clauses can be sent to the remote
	 * server and which can't.
	 */
	classifyConditions(root, baserel, fpinfo->dist_keyatt, baserel->baserestrictinfo,
					   &fpinfo->shard_conds, &fpinfo->global_conds);

	/* We don't consider data skew across table shards in query planning.
     * i.e. table shards are roughly equal sized.  Based on this assumption
     * we estimate scanning cost for an arbitrary shard using statistics
     * collected for a local partition.
     */
	fpinfo->shardrel = build_shardrel(root, baserel, relname);
	/*
	 * Identify which attributes will need to be retrieved from the remote
	 * server. Only consider simple column targets for now so no need to invoke 
     * expression_tree_walker.  
	 */
	fpinfo->attrs_used = NULL;
	ListCell *lc;
	foreach (lc, baserel->reltarget->exprs)
	{
		Expr *node = (Expr *)lfirst(lc);
		if (IsA(node, Var))
		{
			Var *var = (Var *)node;
			if (var->varno == baserel->relid && var->varlevelsup == 0)
				fpinfo->attrs_used =
					bms_add_member(fpinfo->attrs_used,
								   var->varattno - FirstLowInvalidHeapAttributeNumber);
		}
		Assert(!IsA(node, Query));
	}
	/*
	 * Set cached relation costs to some negative value, so that we can detect
	 * when they are set to some sensible costs during one (usually the first)
	 * of the calls to estimate_path_cost_size().
	 */
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;
	/*
	 * We estimate the rows and width of scanning by checking against local
	 * partition info.
	 */
	Cost startup_cost;
	Cost row_cost;
	Cost total_cost;
	set_baserel_size_estimates(root, fpinfo->shardrel);
	fpinfo->rows = fpinfo->shardrel->rows;
	fpinfo->width = fpinfo->shardrel->reltarget->width;
	startup_cost = fpinfo->shardrel->reltarget->cost.startup;
	row_cost = fpinfo->shardrel->reltarget->cost.per_tuple;
	total_cost = startup_cost + row_cost * fpinfo->rows;

	fpinfo->startup_cost = fpinfo->fdw_startup_cost + startup_cost;
	fpinfo->total_cost = total_cost + fpinfo->fdw_tuple_cost * fpinfo->rows;
	baserel->rows = fpinfo->rows;
	baserel->reltarget->width = fpinfo->width;

	return;
}

static void
GetForeignPaths(PlannerInfo *root,
				RelOptInfo *baserel,
				Oid foreigntableid)
{
	RelationInfo *fpinfo = (RelationInfo *)baserel->fdw_private;
	ForeignPath *path;
	List *ppi_list;
	ListCell *lc;

	/*
	 * Create simplest ForeignScan path node and add it to baserel.  This path
	 * corresponds to SeqScan path of regular tables (though depending on what
	 * baserestrict conditions we were able to send to remote, there might
	 * actually be an indexscan happening there).  We already did all the work
	 * to estimate cost and size of this path.
	 */
	path = create_foreignscan_path(root, baserel,
								   NULL, /* default pathtarget */
								   fpinfo->rows,
								   fpinfo->startup_cost,
								   fpinfo->total_cost,
								   NIL,  /* no pathkeys */
								   NULL, /* no outer rel either */
								   NULL, /* no extra plan */
								   NIL); /* no fdw_private list */
	add_path(baserel, (Path *)path);

	/* Add paths with pathkeys */
	add_paths_with_pathkeys_for_rel(root, baserel, NULL);
}

static ForeignScan *
GetForeignPlan(PlannerInfo *root,
			   RelOptInfo *foreignrel,
			   Oid foreigntableid,
			   ForeignPath *best_path,
			   List *tlist,
			   List *scan_clauses,
			   Plan *outer_plan)
{
	RelationInfo *fpinfo = (RelationInfo *)foreignrel->fdw_private;
	Index scan_relid;
	List *fdw_private;
	List *remote_exprs = NIL;
	List *local_exprs = NIL;
	List *params_list = NIL;
	List *fdw_scan_tlist = NIL;
	List *fdw_recheck_quals = NIL;
	List *retrieved_attrs;
	StringInfoData sql;
	ListCell *lc;

	if (!IS_SIMPLE_REL(foreignrel))
		 ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("mypg_fdw: shard table is not simple relation.")));

	scan_relid = foreignrel->relid;

	/*
		 * In a base-relation scan, we must apply the given scan_clauses.
		 *
		 * Separate the scan_clauses into those that can be executed remotely
		 * and those that can't.  baserestrictinfo clauses that were
		 * previously determined to be safe or unsafe by classifyConditions
		 * are found in fpinfo->remote_conds and fpinfo->local_conds. Anything
		 * else in the scan_clauses list will be a join clause, which we have
		 * to check for remote-safety.
		 *
		 * Note: the join clauses we see here should be the exact same ones
		 * previously examined by GetForeignPaths.  Possibly it'd be
		 * worth passing forward the classification work done then, rather
		 * than repeating it here.
		 *
		 * This code must match "extract_actual_clauses(scan_clauses, false)"
		 * except for the additional decision about remote versus local
		 * execution.
		 */
	foreach (lc, scan_clauses)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		/* Ignore any pseudoconstants, they're dealt with elsewhere */
		if (rinfo->pseudoconstant)
			continue;

		if (list_member_ptr(fpinfo->remote_conds, rinfo))
			remote_exprs = lappend(remote_exprs, rinfo->clause);
		else if (list_member_ptr(fpinfo->local_conds, rinfo))
			local_exprs = lappend(local_exprs, rinfo->clause);
		else if (is_foreign_expr(root, foreignrel, rinfo->clause))
			remote_exprs = lappend(remote_exprs, rinfo->clause);
		else
			local_exprs = lappend(local_exprs, rinfo->clause);
	}
	
	
	/*
		 * For a base-relation scan, we have to support EPQ recheck, which
		 * should recheck all the remote quals.
		 */
	fdw_recheck_quals = remote_exprs;
}

#define REL_ALIAS_PREFIX "r"
/* Handy macro to add relation name qualification */
#define ADD_REL_QUALIFIER(buf, varno) \
	appendStringInfo((buf), "%s%d.", REL_ALIAS_PREFIX, (varno))
#define SUBQUERY_REL_ALIAS_PREFIX "s"
#define SUBQUERY_COL_ALIAS_PREFIX "c"

/*
 * Examine each qual clause in input_conds, and classify them into two groups,
 * which are returned as two lists:
 *	- shard_conds contains expressions that can be evaluated on a single shard
 *	- global_conds contains expressions that should be evaluated on all shards.
 */
void classifyConditions(PlannerInfo *root,
						AttrNumber distkey_att,
						RelOptInfo *baserel,
						List *input_conds,
						List **shard_conds,
						List **global_conds)
{
	ListCell *lc;

	*shard_conds = NIL;
	*global_conds = NIL;

	foreach (lc, input_conds)
	{
		RestrictInfo *ri = lfirst_node(RestrictInfo, lc);
		
		if (is_shard_expr(baserel, distkey_att, ri->clause))
			*shard_conds = lappend(*shard_conds, ri);
		else
			*global_conds = lappend(*global_conds, ri);
	}
}

/*
 * Returns true if given expr is safe to evaluate on the foreign server.
 */
static bool
is_shard_expr(RelOptInfo *baserel, AttrNumber distkey_att, Node *expr)
{
	RelationInfo *fpinfo = (RelationInfo *)(baserel->fdw_private);

	if (expr == NULL)
		return true;

	/*
	 * Current sharding solutions do not support aggregations so we don't 
     * care about upper relations. 
	 */
	switch (nodeTag(expr))
	{
	case T_Var:
	{
		Var *var = (Var *)expr;

		if (var->varno != baserel->relid)
			elog(ERROR, "mypg_fdw: restriction variable does not reference foreign table. ");

		return var->varattno == distkey_att ? true : false;
	}
	case T_Const:
	{
		break;
	}
	case T_BoolExpr:
	{
		BoolExpr *b = (BoolExpr *)expr;
		ListCell *lc;
		foreach (lc, b->args)
		{
			if (!is_shard_expr(baserel, distkey_att, (Expr *)lfirst(lc)))
				return false;
		}
		break;
	}
	case T_NullTest:
	{
		NullTest *nt = (NullTest *)expr;

		/*
		 * Recurse to input subexpressions.
		 */
		if (!is_shard_expr(baserel, distkey_att, (Expr *)nt->arg))
			return false;
	}
	break;
	default:
		elog(ERROR, "mypg_fdw: unsupported type found in restriction expression. ");
	}

	/*
	 * An expression which includes any mutable functions can't be sent over
	 * because its result is not stable.  For example, sending now() remote
	 * side could cause confusion from clock offsets.  Future versions might
	 * be able to make this choice with more granularity.  (We check this last
	 * because it requires a lot of expensive catalog lookups.)
	 */
	if (contain_mutable_functions((Node *)expr))
		return false;

	/* OK to evaluate on the remote server */
	return true;
}
