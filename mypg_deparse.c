#include "postgres.h"

#include "mypg_fdw.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "optimizer/clauses.h"
#include "optimizer/prep.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

static void
deparseSelectTargets(StringInfo buf,
				  RangeTblEntry *rte,
				  Index rtindex,
				  Relation rel,
				  bool is_returning,
				  Bitmapset *attrs_used,
				  bool qualify_col,
				  List **retrieved_attrs)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	bool		have_wholerow;
	bool		first;
	int			i;

	*retrieved_attrs = NIL;

	/* If there's a whole-row reference, we'll need all the columns. */
	have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
								  attrs_used);

	first = true;
	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		if (have_wholerow ||
			bms_is_member(i - FirstLowInvalidHeapAttributeNumber,
						  attrs_used))
		{
			if (!first)
				appendStringInfoString(buf, ", ");
			else if (is_returning)
				appendStringInfoString(buf, " RETURNING ");
			first = false;

			deparseColumnRef(buf, rtindex, i, rte, qualify_col);

			*retrieved_attrs = lappend_int(*retrieved_attrs, i);
		}
	}
	/* Don't generate bad syntax if no undropped columns */
	if (first && !is_returning)
		appendStringInfoString(buf, "NULL");
}

void
deparseSimpleSelectStmt(StringInfo buf, PlannerInfo *root, RelOptInfo *relopt,
						List *remote_conds, List *pathkeys,
						List **retrieved_attrs, List **params_list)
{
	RelationInfo   *fpinfo;
	RangeTblEntry  *rte;
	Relation 		rel;
	bool			is_first = true;
	char	   	   *delim = " ";

	fpinfo = (RelationInfo *) relopt->fdw_private;

	appendStringInfoString(buf, "SELECT ");
	/*
	 * For a base relation fpinfo->attrs_used gives the list of columns
	 * required to be fetched from the foreign server.
	 */
	rte = planner_rt_fetch(relopt->relid, root);

	rel = heap_open(rte->relid, NoLock);

	deparseSelectTargets(buf, rte, relopt->relid, rel, false,
					  	 fpinfo->attrs_used, false, retrieved_attrs);

	/* Construct FROM clause */
	appendStringInfoString(buf, " FROM ");

	deparseRelation(buf, rel);

	heap_close(rel, NoLock);

	if (remote_conds != NIL)
	{
		appendStringInfoString(buf, " WHERE ");
	}

	foreach(lc, remote_conds)
	{
		Expr	   *expr = (Expr *) lfirst(lc);

		/* Extract clause from RestrictInfo, if required */
		if (IsA(expr, RestrictInfo))
			expr = ((RestrictInfo *) expr)->clause;

		/* Connect expressions with "AND" and parenthesize each condition. */
		if (!is_first)
			appendStringInfoString(buf, " AND ");

		appendStringInfoChar(buf, '(');
		deparseExpr(expr, context);
		appendStringInfoChar(buf, ')');

		is_first = false;
	}

	/* Add ORDER BY clause if we found any useful pathkeys */
	if (pathkeys)
		appendStringInfoString(buf, " ORDER BY");
	
	foreach(lcell, pathkeys)
	{
		PathKey    *pathkey = lfirst(lcell);
		Expr	   *em_expr;

		em_expr = find_em_expr_for_rel(pathkey->pk_eclass, baserel);
		Assert(em_expr != NULL);

		appendStringInfoString(buf, delim);
		deparseExpr(em_expr, context);
		if (pathkey->pk_strategy == BTLessStrategyNumber)
			appendStringInfoString(buf, " ASC");
		else
			appendStringInfoString(buf, " DESC");

		if (pathkey->pk_nulls_first)
			appendStringInfoString(buf, " NULLS FIRST");
		else
			appendStringInfoString(buf, " NULLS LAST");

		delim = ", ";
	}
}

#define REL_ALIAS_PREFIX "r"
/* Handy macro to add relation name qualification */
#define ADD_REL_QUALIFIER(buf, varno) \
	appendStringInfo((buf), "%s%d.", REL_ALIAS_PREFIX, (varno))
#define SUBQUERY_REL_ALIAS_PREFIX "s"
#define SUBQUERY_COL_ALIAS_PREFIX "c"

/*
 * Examine an expression to decide where it should be evaluated: on one shard, on all
 * shards, or locally. The recursive algorithm works by decrementing the value passed
 * in by c when we determine the expression can't be evaluated on one shard or can't
 * even remotely.
 */
void
walk_foreign_expr(RelOptInfo *baserel, AttrNumber distkey_att, Node *expr, int *c)
{
	if (expr == NULL)
		return;

	/*
	 * Current sharding solutions do not support aggregations and we only care about
	 * equal conditions.
	 */
	switch (nodeTag(expr))
	{
	case T_Var:
	{
		Var *var = (Var *)expr;

		if (var->varno != baserel->relid)
			*c = 0;
		else if (*c == 2 && var->varattno != distkey_att)
			(*c)--;

		return;
	}
	case T_Const:
	{
		return;
	}
	case T_BoolExpr:
	{
		BoolExpr *b = (BoolExpr *)expr;
		ListCell *lc;

		if (*c == 2)
			(*c)--;

		foreach (lc, b->args)
		{
			walk_foreign_expr(baserel, distkey_att, (Node *)lfirst(lc), c);
			if (*c == 0)
				return;
		}
		return;
	}
	case T_OpExpr:
	case T_DistinctExpr:
	{
		/* For now only consider equal operators */
		OpExpr	   *oe = (OpExpr *) expr;

		walk_foreign_expr(baserel, distkey_att, (Node *)oe->args, c);
		return;
	}
	case T_List:
	{
		List	   *l = (List *) expr;
		ListCell   *lc;

		foreach(lc, l)
		{
 			walk_foreign_expr(baserel, distkey_att, (Node *)lfirst(lc), c);
			if (*c == 0)
				return;
		}
		return;
	}
	case T_NullTest:
	{
		NullTest *nt = (NullTest *)expr;

		walk_foreign_expr(baserel, distkey_att, (Node *)nt->arg, c);
		return;
	}
	default:
		elog(ERROR, "mypg_fdw: unsupported type found in restriction expression. ");
	}

	/* OK to evaluate on the remote server */
	return;
}


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
						List **global_conds,
						List **local_conds)
{
	ListCell       *lc;

	*shard_conds  = NIL;
	*global_conds = NIL;
	*local_conds  = NIL;

	foreach (lc, input_conds)
	{
		RestrictInfo *ri = lfirst_node(RestrictInfo, lc);
		int c = 2;

		c = walk_foreign_expr(baserel, distkey_att, ri->clause, &c);
		
		if (c == 2)
			*shard_conds = lappend(*shard_conds, ri);
		else if (c == 1)
			*global_conds = lappend(*global_conds, ri);
		else
			*local_conds = lappend(*local_conds, ri);
	}
}
