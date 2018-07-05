#ifndef MYPG_CONFIG_H
#define MYPG_CONFIG_H

#define MYPG_EXTENSION_NAME             "mypg_sharding"

struct FormData_mypg_tables
{
    char[64]	relname;
	int16       distkey;
	int16       modulo;
	double      rows;
	int16       width;
	double      tuples;
	text        createsql; 
};
typedef struct FormData_mypg_tables *Form_mypg_tables;
#define MYPG_TABLES                     "tables"
#define Natts_mypg_tables               7
#define Anum_mypg_tables_relname        1
#define Anum_mypg_tables_distkey        2
#define Anum_mypg_tables_modulo         3
#define Anum_mypg_tables_createsql      4
#define Anum_mypg_tables_rows           5
#define Anum_mypg_tables_width          6
#define Anum_mypg_tables_tuples         7

struct FormData_mypg_partitions
{
    Oid         localid;
    char[64]    relname;
    int         p;
    char[64]    node;
};
typedef struct FormData_mypg_partitions *Form_mypg_partitions;

#define MYPG_PARTITIONS                 "partitions"
#define Natts_mypg_partitions           4
#define Anum_mypg_partitions_localid    1
#define Anum_mypg_partitions_relname    2
#define Anum_mypg_partitions_p          3
#define Anum_mypg_partitions_node       4


#endif