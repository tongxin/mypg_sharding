/* ------------------------------------------------------------------------
 *
 * mypg_sharding.sql
 *   sharding and clustering utilities for mypg
 *
 * Copyright (c) 2018, Tongxin Bai
 *
 * ------------------------------------------------------------------------
 */
          
-- complain if script is sourced in psql, rather than via CREATE EXTENSION
-- \echo Use "CREATE EXTENSION mypg_sharding" to load this file. \quit

-- We define several GUCs (though user can see them in SHOW if she sets it
-- explicitly even without loaded lib) and have to inform pathman that we want
-- shardman's COPY FROM, so it makes sense to load the lib on server start.
DO $$
BEGIN
-- -- Yes, malicious user might have another extension containing 'pg_shardman'...
-- -- Probably better just call no-op func from the library
	IF strpos(current_setting('shared_preload_libraries'), 'mypg_sharding') = 0 THEN
		RAISE EXCEPTION 'mypg_sharding is required to be loaded via shared_preload_libraries.';
	END IF;
END
$$;

-- This extension defines core data and functions for sharding controllers.

-- The global sharding state is maintained in the following defined tables.

-------------------------------------------------------------------------------
-- Node group
--
-- The database nodes are organized in non-overlapping node groups which 
-- define the scopes of data distribution for all sharding related operations.
-- For instance, the cluster is comprised of node1, node2, ... node10, where
-- node1 ... node7 are in group1, and node8 ... node10 are in group2. Note,
-- here a node refers a database instance, and hence multiple nodes can be
-- arranged to reside in one physical host. Grouping nodes makes it simple to
-- implement table collocation as related tables can be sharded in the same way
-- in the same node group.
-------------------------------------------------------------------------------
CREATE TABLE	nodegroup (
	gid			smallint	NOT NULL,
	node_name	text		NOT NULL,
	node_idx	smallint	NOT NULL,	
	UNIQUE		(gid, node_name)
);

-- Node information
CREATE TABLE 	node (
	node_name	text		PRIMARY KEY,
	host_addr	text		NOT NULL,
	system_id	bigint		NOT NULL	UNIQUE,
	coninfo		text		NOT NULL,		-- may require superuser role
);

-- Table information
CREATE TABLE tableinfo (
	table_name	text	PRIMARY KEY,  		-- sharding table's global name
	sharding_key	text,					-- partition key expression
	gid			smallint,					-- assigned node group
	create_sql	text       					-- sql to create the table
);

-- Make the above config tables dump-able
-- SELECT pg_catalog.pg_extension_config_dump('mypg.cluster_nodes', '');
-- SELECT pg_catalog.pg_extension_config_dump('mypg.tables', '');
-- SELECT pg_catalog.pg_extension_config_dump('mypg.partitions', '');

-- Sharding interface functions
CREATE FUNCTION create_nodegroup (
	nodes	text[]
)
RETURNS smallint AS $$
DECLARE
	max_gid		smallint;
	valid_nodes	text[];
BEGIN
	SET TRANSACTION ISOLATION LEVEL repeatable_read;
	max_gid :=	SELECT	max(gid)
				FROM	mypg.nodegroup;
	
	nodes	:=	SELECT 	array_agg(ns.node_name)
				FROM	(SELECT DISTINCT unnest(nodes) node_name) ns;

	valid_nodes :=
				SELECT	array_agg(node_name)
				FROM	node
				EXCEPT
				SELECT 	node_name
				FROM	nodegroup;

	IF NOT nodes <@ valid_nodes
		RAISE EXCEPTION 'Some nodes are either unknown or already assigned group.'
	END IF;

	INSERT INTO mypg.nodegroup 	(gid, node_name, node_idx)
		SELECT	max_gid, unnest(nodes), generate_series(1, count(nodes)); 

	RETURNING 	gid;
END 
$$ LANGUAGE plpgsql;

-- Add new node to the sharding cluster. Some constraints include: ...
CREATE FUNCTION register_node (
	newnode_name 	text,
	newnode_coninfo text,
	newnode_group		smallint
)
RETURNS record AS $$
DECLARE
	nodename		text;
	tablename		text;
	createsql		text;
	shardkey		text;
	partname		text;
	fdw_partname	text;
	shardcount		int;
	shardidx		int[];
	shardnodes		text[];
	sys_id			bigint;
	coninfo			text;
	server_opts		text;
	um_opts			text;
	new_server_opts text;
	new_um_opts		text;
	fdws			text = '';
	usms			text = '';
	create_tables	text = '';
	create_partitions text = '';
	create_fdws		text = '';
	table_attrs		text;
	res_res			text;
	res_msg			text;
BEGIN
	-- Error if the node already exists.
	IF EXISTS (
		SELECT 	1 
		FROM 	mypg.nodeinfo
		WHERE 	node_name = newnode_name
	)
	THEN
		RAISE EXCEPTION 'Node % already exists.', newnode_name;
	END IF;

	-- Verify the required node group is valid.
	IF NOT EXISTS (
		SELECT	1
		FROM	mypg.nodegroup
		WHERE	groupid = newnode_group
	)
	THEN
		RAISE EXCEPTION 'Node group % does not exist.', newnode_group;
	END IF;

	-- Insert new node in the nodeinfo table.
	INSERT INTO	mypg.nodeinfo
			(node_name,		system_id, 	coninfo,			group			)
	VALUES 	(newnode_name, 	0, 			newnode_coninfo,	newnode_group	);

	-- A balanced group becomes unbalanced once a new node is added to it
	UPDATE 	mypg.nodegroup
	SET		balanced = FALSE
	WHERE	groupid = newnode_group
		AND	balanced = TRUE;
	
	-- Validate the accessibility of the new node.
	IF NOT EXISTS (
		SELECT * FROM mypg.exec_remote_query('SELECT 1;', newnode_coninfo);
	)
	THEN
		RAISE EXCEPTION 'Connection failed with connection string ''%''.', newnode_coninfo;
	END IF;

	-- Retrieve the system id from the new node.
 	sys_id := mypg.exec_remote_query('SELECT mypg.get_system_id();', newnode_coninfo);
	IF EXISTS (SELECT 1 FROM mypg.nodeinfo WHERE system_id = sys_id)
	THEN
		RAISE EXCEPTION 'Node with system id % already is in the cluster.' sys_id;
	END IF;

	-- Update the node's system_id in the cluster_nodes table.
	UPDATE	mypg.nodeinfo
	SET 	system_id = sys_id
	WHERE	node_name = newnode_name;

	-- Add foreign servers for connection between the new and existing nodes.
	SELECT 	*
	FROM	mypg.conninfo_to_postgres_fdw_opts(conn_string_effective)
	INTO	new_server_opts,
			new_um_opts;
	
	FOR 	nodename, coninfo, server_opts, um_opts IN
	SELECT 	node_name,
			coninfo,
			(mypg.conninfo_to_postgres_fdw_opts(coninfo)).*
	FROM 	mypg.nodeinfo
	WHERE 	node_name <> newnode_name AND group = newgroup
	LOOP
		-- Create foreign server for new node at all other nodes 
		-- and servers at new node for all other nodes, all within the same node group
		fdws := format('%s%s:CREATE SERVER %s FOREIGN DATA WRAPPER postgres_fdw %s;
			 			  %s:CREATE SERVER %s FOREIGN DATA WRAPPER postgres_fdw %s;',
			 fdws, newnode_name, nodename, server_opts,
			 	   nodename, newnode_name, new_server_opts);

		-- Create user mapping for this servers
		usms := format('%s%s:CREATE USER MAPPING FOR CURRENT_USER SERVER %s %s;
			 			  %s:CREATE USER MAPPING FOR CURRENT_USER SERVER %s %s;',
			 usms, newnode_name, node.node_name, um_opts,
			      node.node_name, newnode_name, new_um_opts);
	END LOOP;
	-- Broadcast command for creating foreign servers
	PERFORM shardman.broadcast(fdws);
	-- Broadcast command for creating user mapping for this servers
	PERFORM shardman.broadcast(usms);

	-- On the new node, create FDWs for all existing distributed tables.
	FOR		tablename, createsql, shardkey, shardcount, shardnodes, shardidx IN 
	SELECT	t.table_name,
			first(t.create_sql),
			first(t.sharding_key),
			count(*)
			array_agg(s.node_name)
			array_agg(s.balanced_idx)			
	FROM 	mypg.tableinfo	t
	JOIN	mypg.nodeinfo	s
	WHERE	s.balanced_idx	is not null
	ON		t.group 	= s.group
	GROUP BY t.table_name
	LOOP
		create_tables := format('%s{%s:%s}',
			create_tables, newnode_name, createsql);
		create_partitions := format('%s%s:SELECT create_hash_partitions(%L,%L,%L);',
			create_partitions, newnode_name, tablename, shardkey, shardcount);
		SELECT mypg.reconstruct_table_attrs(tablename) INTO table_attrs;

		FOR	i IN 1..array_length(shardnodes, 1)
	    LOOP
			nodename := shardnodes[i];
			partname := format('%s_%s', tablename, shardidx[i]);
			fdw_partname := format('%s_fdw', partname);
			create_fdws := format(
				'%s%s:CREATE FOREIGN TABLE %I %s SERVER %s OPTIONS (table_name %L);',
				create_fdws, newnode_name, fdw_partname, table_attrs, nodename, partname);
			create_fdws := format(
				'%s%s:SELECT replace_hash_partition(%I::regclass, %I::regclass);',
				partname, fdw_partname);
			create_fdws := format(
				'%s%s:TRUNCATE TABLE %I;', partname);
		END LOOP;
	END LOOP;

	-- Broadcast create table commands
	PERFORM mypg.broadcast(create_tables);
	-- Broadcast create hash partitions command
	PERFORM mypg.broadcast(create_partitions, iso_level => 'read committed');
	-- Create foreign tables for all foreign partitions on the new node
	PERFORM mypg.broadcast(create_fdws);

	RETURN newnode_name;
END
$$ LANGUAGE plpgsql;


CREATE FUNCTION rebalance_nodegroup(group int)
RETURNS void AS $$
DECLARE
	nodename	text;
	sids		int[];
	sid_min		int;
	sid_max		int;
	sid_count	int;
BEGIN
	-- Check validity of the argument
	IF NOT EXISTS (
		SELECT 	1
		FROM	nodeinfo
		WHERE	nodegroup = group
	)
	THEN
		RAISE EXCEPTION 'Invalid node group id %.', group;
	END IF;

	-- Check if it's necessary to do data rebalance.
	IF NOT EXISTS (
		SELECT	1
		FROM	nodeinfo
		WHERE	nodegroup = group
			AND	balanced_idx IS NULL
	)
	THEN
		RAISE EXCEPTION 'All nodes are balanced in this group. No more work is needed.';
	END IF;

	-- Validate the existing non-null balanced_idx's form a sequence 1..s
	SELECT 	array_agg(balanced_idx ORDER BY balanced_idx),
			min(balanced_idx),
			max(balanced_idx),
			count(*)
	INTO	sids, sid_min, sid_max, sid_count 
	FROM	nodeinfo
	WHERE	nodegroup = group
		AND	balanced_idx IS NOT NULL

	IF sid_min <> 1 OR sid_max <> sid_count
	THEN
		RAISE EXCEPTION 'Existing sharding is broken in node group %, requiring further investigation', group;
	END IF;


END
$$ LANGUAGE plpgsql;

-- Shard table across all the available nodes
CREATE FUNCTION create_hash_partitions(rel_name name, expr text)
RETURNS void AS $$
DECLARE
	create_table text;
	node shardman.nodes;
	node_ids int[];
	node_id int;
	part_name text;
	fdw_part_name text;
	table_attrs text;
	srv_name text;
	create_tables text = '';
	create_partitions text = '';
	create_fdws text = '';
	replace_parts text = '';
	i int;
	n_nodes int;
BEGIN
	IF EXISTS(SELECT relation FROM shardman.tables WHERE relation = rel_name)
	THEN
		RAISE EXCEPTION 'Table % is already sharded', rel_name;
	END IF;

	IF (SELECT count(*) FROM shardman.nodes) = 0 THEN
		RAISE EXCEPTION 'Please add some nodes first';
	END IF;

	-- Check right away to avoid unneccessary recover()
	PERFORM shardman.check_max_replicas(redundancy);

	-- Generate SQL statement creating this table
	SELECT shardman.gen_create_table_sql(rel_name) INTO create_table;

	INSERT INTO shardman.tables (relation,sharding_key,partitions_count,create_sql) values (rel_name,expr,part_count,create_table);

	-- Create parent table and partitions at all nodes
	FOR node IN SELECT * FROM shardman.nodes
	LOOP
		-- Create parent table at all nodes
		create_tables := format('%s{%s:%s}',
			create_tables, node.id, create_table);
		create_partitions := format('%s%s:select create_hash_partitions(%L,%L,%L);',
			create_partitions, node.id, rel_name, expr, part_count);
	END LOOP;

	-- Broadcast create table commands
	PERFORM shardman.broadcast(create_tables);
	-- Broadcast create hash partitions command
	PERFORM shardman.broadcast(create_partitions, iso_level => 'read committed');

	-- Get list of nodes in random order
	SELECT ARRAY(SELECT id from shardman.nodes ORDER BY random()) INTO node_ids;
	n_nodes := array_length(node_ids, 1);

	-- Reconstruct table attributes from parent table
	SELECT shardman.reconstruct_table_attrs(rel_name::regclass) INTO table_attrs;

	FOR i IN 0..part_count-1
	LOOP
		-- Choose location of new partition
		node_id := node_ids[1 + (i % n_nodes)]; -- round robin
		part_name := format('%s_%s', rel_name, i);
		fdw_part_name := format('%s_fdw', part_name);
		-- Insert information about new partition in partitions table
		INSERT INTO shardman.partitions (part_name, node_id, relation) VALUES (part_name, node_id, rel_name);
		-- Construct name of the server where partition will be located
		srv_name := format('node_%s', node_id);

		-- Replace local partition with foreign table at all nodes except owner
		FOR node IN SELECT * FROM shardman.nodes WHERE id<>node_id
		LOOP
			-- Create foreign table for this partition
			create_fdws := format(
				'%s%s:SELECT shardman.replace_real_with_foreign(%s, %L, %L);',
				create_fdws, node.id, node_id, part_name, table_attrs);
		END LOOP;
	END LOOP;

	-- Broadcast create foreign table commands
	PERFORM shardman.broadcast(create_fdws, iso_level => 'read committed');
	-- Broadcast replace hash partition commands
	PERFORM shardman.broadcast(replace_parts);

	IF redundancy <> 0
	THEN
		PERFORM shardman.set_redundancy(rel_name, redundancy, copy_data => false);
	END IF;
END
$$ LANGUAGE plpgsql;

-- Construct postgres_fdw options based on the given connection string
CREATE FUNCTION conninfo_to_postgres_fdw_opts(IN conn_string text,
	OUT server_opts text, OUT um_opts text) RETURNS record AS $$
DECLARE
	conn_string_keywords text[];
	conn_string_vals text[];
	server_opts_first_time_through bool = true;
	um_opts_first_time_through bool = true;
BEGIN
	server_opts := '';
	um_opts := '';
	SELECT * FROM mypg.pq_conninfo_parse(conn_string)
	  INTO conn_string_keywords, conn_string_vals;
	FOR i IN 1..array_upper(conn_string_keywords, 1) LOOP
		IF conn_string_keywords[i] = 'client_encoding' OR
			conn_string_keywords[i] = 'fallback_application_name' THEN
			CONTINUE; /* not allowed in postgres_fdw */
		ELSIF conn_string_keywords[i] = 'user' OR
			conn_string_keywords[i] = 'password' THEN -- user mapping option
			IF NOT um_opts_first_time_through THEN
				um_opts := um_opts || ', ';
			END IF;
			um_opts_first_time_through := false;
			um_opts := um_opts ||
				format('%s %L', conn_string_keywords[i], conn_string_vals[i]);
		ELSE -- server option
			IF NOT server_opts_first_time_through THEN
				server_opts := server_opts || ', ';
			END IF;
			server_opts_first_time_through := false;
			server_opts := server_opts ||
				format('%s %L', conn_string_keywords[i], conn_string_vals[i]);
		END IF;
	END LOOP;

	-- OPTIONS () is syntax error, so add OPTIONS only if we really have opts
	IF server_opts != '' THEN
		server_opts := format(' OPTIONS (%s)', server_opts);
	END IF;
	IF um_opts != '' THEN
		um_opts := format(' OPTIONS (%s)', um_opts);
	END IF;
END $$ LANGUAGE plpgsql STRICT;

-- Parse connection string. This function is used by
-- conninfo_to_postgres_fdw_opts to construct postgres_fdw options list.
CREATE FUNCTION pq_conninfo_parse(IN conninfo text, OUT keys text[], OUT vals text[])
	RETURNS record AS 'mypg_sharding' LANGUAGE C STRICT;

-- Generate based on information from catalog SQL statement creating this table
CREATE FUNCTION gen_create_table_sql(relation text)
RETURNS text AS 'mypg_sharding' LANGUAGE C STRICT;

-- Generate pg_dump'ed sql commands for table copying.
CREATE FUNCTION gen_copy_table_sql(relation text)
RETURNS text AS 'mypg_sharding' LANGUAGE C STRICT;

-- Reconstruct table attributes for foreign table
CREATE FUNCTION reconstruct_table_attrs(relation regclass)
RETURNS text AS 'mypg_sharding' LANGUAGE C STRICT;

CREATE FUNCTION broadcast(cmds text,
						  ignore_errors bool = false,
						  two_phase bool = false,
						  sequential bool = false,
						  iso_level text = null)
RETURNS exec_result AS 'mypg_sharding' LANGUAGE C;

CREATE FUNCTION copy_table_data(rel text, node_name text)
RETURNS exec_result AS 'mypg_sharding' LANGUAGE C STRICT;

-- Check from configuration parameters if node plays role of shardlord
CREATE FUNCTION is_master()
	RETURNS bool AS 'mypg_sharding' LANGUAGE C STRICT;

-- Returns this node's node name
CREATE FUNCTION node_name() RETURNS text
AS 'mypg_sharding' LANGUAGE C STRICT;

-- Disable writes to the partition, if we are not replica. This is handy because
-- we use replication to copy table.
CREATE FUNCTION write_protection_on(part regclass) RETURNS void AS $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'write_protection' AND
												  tgrelid = part) THEN
		EXECUTE format('CREATE TRIGGER write_protection BEFORE INSERT OR UPDATE OR DELETE ON
					   %I FOR EACH STATEMENT EXECUTE PROCEDURE mypg.deny_access();',
					   part::name);
	END IF;
END
$$ LANGUAGE plpgsql;

-- Enable writes to the partition back again
CREATE FUNCTION write_protection_off(part regclass) RETURNS void AS $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'write_protection' AND
											  tgrelid = part) THEN
		EXECUTE format('DROP TRIGGER write_protection ON %I', part::name);
	END IF;
END
$$ LANGUAGE plpgsql;

-- Trigger procedure prohibiting modification of the table
CREATE FUNCTION deny_access() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'This partition was moved to another node. Run mypg.recover(), if this error persists.';
END
$$ LANGUAGE plpgsql;

-- Returns PostgreSQL system identifier (written in control file)
CREATE FUNCTION get_system_id()
    RETURNS bigint AS 'mypg_sharding' LANGUAGE C STRICT;

-- Initialization 

