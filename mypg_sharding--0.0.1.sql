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

-- Metadata tables

-- Node state
CREATE TABLE nodestate (
	node_name text NOT NULL,       -- the node name
	current text NOT NULL,         -- local node state: INIT, ACTIVE, or INACTIVE
	epoch integer NOT NULL         -- starting from 1, increased everytime a cluster configuration change occurs 
);

-- Server nodes
CREATE TABLE cluster_nodes (
	node_name text NOT NULL UNIQUE,
	system_id bigint NOT NULL UNIQUE,
	host text NOT NULL,
	port text NOT NULL,
	dbname text NOT NULL,
	UNIQUE (host, port)
);

-- Distributed tables
CREATE TABLE tables (
	relation text PRIMARY KEY,     -- table name
	key_col text,                  -- sharding key column
	modulo integer,                -- maximum number of distributed partitions
	create_sql text NOT NULL       -- sql to create the table
--	create_rules_sql text          -- sql to create rules for shared table
);

CREATE TABLE partitions (
	relation text,                 -- table name
	node_name text,                -- node id for the partition
	r integer                      -- modulo index for the partition
);

-- Make the above config tables dump-able
SELECT pg_catalog.pg_extension_config_dump('mypg.cluster_nodes', '');
SELECT pg_catalog.pg_extension_config_dump('mypg.tables', '');
SELECT pg_catalog.pg_extension_config_dump('mypg.partitions', '');

create type exec_result as (res text, msg text);

-- Sharding interface functions

CREATE FUNCTION add_node (name_ text, host_ text, port_ text) 
RETURNS table(res text, msg text) AS $$
DECLARE
	node mypg.cluster_nodes;
	sys_id bigint;
	currentdb text;
	current_epoch int;
	copy_nodes_msg text := '';
	init_nodestate_msg text := '';
	insert_node_msg text := '';
	copy_tables_msg text := '';
	update_epoch_msg text := '';
	res_res text;
	res_msg text;
BEGIN
	-- Fail if this command is not run at the master node.
	IF NOT mypg.is_master()
	THEN 
		RAISE EXCEPTION 'Only master node can invoke add_node.';
	END IF;

	-- Error if the node is already added.
	IF EXISTS (
		SELECT 1 
		FROM mypg.cluster_nodes
		WHERE node_name = name_)
	THEN
		RAISE EXCEPTION 'Node % already exists.', name_;
	END IF;
	
	IF EXISTS (
		SELECT 1 
		FROM mypg.cluster_nodes
		WHERE host_ = host and port_ = port)
	THEN
		RAISE EXCEPTION 'Node exists with host=% and port=%', host_, port_;
	END IF;

	SELECT current_database() INTO currentdb;

	-- Insert new node in the cluster_nodes table. Update master's epoch number.
	INSERT INTO mypg.cluster_nodes (node_name, system_id, host, port, dbname)
	VALUES (name_, 0, host_, port_, currentdb); 

	-- Check if the new node is in INIT state
	SELECT * INTO res_res, res_msg 
	FROM mypg.broadcast(format('%s:SELECT current FROM mypg.nodestate WHERE node_name = ''%s'';',
								name_, name_));
	IF res_res IS NULL OR res_res <> 'INIT'
	THEN
		RAISE EXCEPTION 'Node % is not in INIT state and cannot be added: %',
						name_, res_res;
	END IF;

	-- Retrieve the system id from the new node.
 	SELECT * INTO res_res, res_msg
	FROM mypg.broadcast(format('%s:SELECT mypg.get_system_id();', name_));
	IF res_msg IS NOT NULL
	THEN
		RAISE EXCEPTION 'Remote error in mypg.get_system_id(): %', res_msg;
	END IF;
	sys_id := res_res;
	IF EXISTS (SELECT 1 FROM mypg.cluster_nodes WHERE system_id = sys_id)
	THEN
		RAISE EXCEPTION 'System id has been taken.';
	END IF;
	-- Update the node's system_id in the cluster_nodes table.
	UPDATE mypg.cluster_nodes
	SET system_id = sys_id
	WHERE node_name = name_;

 	-- Copy the cluster metadata off to the new node.
	SELECT * INTO res_res, res_msg
	FROM mypg.copy_table_data('mypg.cluster_nodes', name_);
	IF res_msg IS NOT NULL
	THEN
		RAISE EXCEPTION 'Failed to copy mypg.cluster_nodes to %: %', name_, res_msg;
	ELSE
		RAISE INFO 'Successful copy of mypg.cluster_nodes to %', name_;
	END IF;

	SELECT epoch INTO current_epoch
	FROM mypg.nodestate
	LIMIT 1;

	init_nodestate_msg :=
		format('%s:UPDATE mypg.nodestate SET current = ''ACTIVE'', epoch = %s WHERE node_name = ''%s'';',
				name_, current_epoch, name_);

	SELECT * INTO res_res, res_msg
	FROM mypg.broadcast(init_nodestate_msg, iso_level => 'READ COMMITTED'); -- needs error handling here
	
	IF res_msg IS NOT NULL
	THEN
		RAISE EXCEPTION 'Failed to update mypg.nodestate on node %: %', name_, res_msg;
	ELSE
		RAISE INFO 'nodestate on % is updated; epoch = %', name_, current_epoch;
	END IF;

	-- Copy the tables metadata to the new node.
	IF EXISTS (
		SELECT 1 FROM mypg.tables) 
	THEN
		copy_tables_msg :=
			format('%s:%s', name_, mypg.gen_copy_table_sql('mypg.tables'));
		PERFORM mypg.broadcast(copy_tables_msg);
	END IF;

	UPDATE mypg.nodestate
	SET epoch = epoch + 1
		RETURNING epoch INTO current_epoch;
	
	-- Update on all cluster nodes to include the new node. 
	FOR node IN 
	SELECT * FROM mypg.cluster_nodes
	LOOP
		insert_node_msg :=
			format('%s%s:INSERT INTO mypg.cluster_nodes VALUES (''%s'', %s, ''%s'', ''%s'', ''%s'');', 
		            insert_node_msg, node.node_name, name_, sys_id, host_, port_, currentdb);
		update_epoch_msg :=
			format('%s%s:UPDATE mypg.nodestate SET epoch = epoch + 1 WHERE node_name = ''%s'' RETURNING epoch;',
		 			update_epoch_msg, node.node_name, node.node_name);
	END LOOP;
	
	SELECT * INTO res_res, res_msg
	FROM mypg.broadcast(insert_node_msg);

	SELECT * INTO res_res, res_msg
	FROM mypg.broadcast(update_epoch_msg, two_phase => true, iso_level => 'READ COMMITTED');
 
	RETURN QUERY
	SELECT 'SUCCESS', format('%I, %I, %I, %I, %I', node_name, system_id, host, port, dbname)
	FROM mypg.cluster_nodes
	WHERE node_name = name_;	
END
$$ LANGUAGE plpgsql;



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

-- Initialize nodestate for master
DO $$
DECLARE
is_master bool;
init_state text;
name_ text;
BEGIN
	is_master := mypg.is_master();
	if is_master THEN
		init_state := 'ACTIVE';
	ELSE
		init_state := 'INIT';
	END IF;
	name_ := mypg.node_name();
	INSERT INTO mypg.nodestate(node_name,current,epoch)
	VALUES (name_, init_state, 0);
END$$;
