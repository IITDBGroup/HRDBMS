package com.exascale.misc;

public enum HrdbmsType {
    REFERENCE, // 0 - reference
    ANTIJOIN, // 1 - AntiJoin
    HMSTRINGSTRING, // 2 - HMStringString
    STRING, // 3 - String
    HMSTRINGINT, // 4 - HMStringInt
    TREEMAP, // 5 - TreeMap
    ALS, // 6 - ALS
    ALI, // 7 - ALI
    HSHM, // 8 - HSHM
    HMF, // 9 - HMF
    ALINDX, // 10 - ALIndx
    ALHSHM, // 11 - ALHSHM
    DOUBLENULL, // 12 - Double null
    DOUBLE, // 13 - Double
    LONGNULL, // 14 - Long null
    LONG, // 15 - Long
    INTEGERNULL, // 16 - Integer null
    INTEGER, // 17 - Integer
    MYDATENULL, // 18 - MyDate null
    MYDATE, // 19 - MyDate
    CASE, // 20 - Case
    CONCATE, // 21 - Concat
    DATEMATH, // 22 - DateMath
    DEMOPERATOR, // 23 - DEMOperator
    DUMMYOPERATOR, // 24 - DummyOperator
    EXCEPT, // 25 - Except
    EXTENDOBJECT, // 26 - ExtendObject
    EXTEND, // 27 - Extend
    FSTNULL, // 28 - FST null
    ADS, // 29 - ADS
    INDEXOPERATOR, // 30 - IndexOperator
    INTERSECT, // 31 - Intersect
    ALOP, // 32 - ALOp
    MULTI, // 33 - Multi
    ALAGOP, // 34 - ALAgOp
    NRO, // 35 - NRO
    NSO, // 36 - NSO
    PROJECT, // 37 - Project
    RENAME, // 38 - Rename
    REORDER, // 39 - Reorder
    ROOT, // 40 - Root
    SELECT, // 41 - Select
    ALF, // 42 - ALF
    HSO, // 43 - HSO
    SORT, // 44 - Sort
    ALB, // 45 - ALB
    INTARRAY, // 46 - IntArray
    SUBSTRING, // 47 - Substring
    TOP, // 48 - Top
    UNION, // 49 - Union
    YEAR, // 50 - Year
    AVG, // 51 - Avg
    COUNTDISTINCT, // 52 - CountDistinct
    COUNT, // 53 - Count
    MAX, // 54 - Max
    MIN, // 55 - Min
    SUM, // 56 - Sum
    SJO, // 57 - SJO
    PRODUCT, // 58 - Product
    NL, // 59 - NL
    HJO, // 60 - HJO
    CNFNULL, // 61 - CNF null
    FST, // 62 - FST
    CNF, // 63 - CNF
    INDEX, // 64 - Index
    FILTER, // 65 - Filter
    STRINGARRAY, // 66 - StringArray
    ALALF, // 67 - ALALF
    HSS, // 68 - HSS
    FILTERNULL, // 69 - Filter null
    BOOLNULL, // 70 - Bool null
    BOOLCLASS, // 71 - Bool class
    NHAS, // 72 - NHAS
    NHRAM, // 73 - NHRAM
    NHRO, // 74 - NHRO
    NRAM, // 75 - NRAM
    NSMO, // 76 - NSMO
    NSRR, // 77 - NSRR
    TSO, // 78 - TSO
    HMOPCNF, // 79 - HMOpCNF
    HMINTOP, // 80 - HMIntOp
    OPERATORNULL, // 81 - Operator null
    INDEXNULL, // 82 - Index null;
    BOOLARRAY, // 83 - BoolArray
    ALALO, // 84 - ALALO
    ALALS, // 85 - ALALS
    ALALB, // 86 - ALALB
    ALRAIK, // 87 - ALRAIK
    RAIK, // 88 - RAIK
    ETSO, // 89 - external table scan
    NULLSTRING, // 90 - NULL String
    ROUTING,  // 91 - Routing
    ALO, // 92 - ArrayList of Objects
    CSVEXTERNALTABLE, // 93 - External Table Implementation
    CSVEXTERNALPARAMS, // 94 - External Table Parameters
    HDFSCSVEXTERNALTABLE; // 95 - HDFS External Table Implementation
    // Make sure to add new values at the end to maintain the above ordinals as they should be.

    public static HrdbmsType fromInt(int value) { return HrdbmsType.values()[value]; }
}
