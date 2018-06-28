#ifndef MYPG_CONFIG_H
#define MYPG_CONFIG_H

#define MYPG_EXTENSION_NAME             "mypg_sharding"

struct FormData_mypg_partitions
{
    Oid         localid;
    NameData    relname;
    int         p;
    NameData    node;
};
typedef struct FormData_mypg_partitions *Form_mypg_partitions;

#define MYPG_PARTITIONS                 "partitions"
#define Natts_mypg_partitions           4
#define Anum_mypg_partitions_localid    1
#define Anum_mypg_partitions_relname    2
#define Anum_mypg_partitions_p          3
#define Anum_mypg_partitions_node       4


#endif