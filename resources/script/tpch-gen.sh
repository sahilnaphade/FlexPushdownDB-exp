# put this under tpch dbgen folder
# input: sf

sf=$1
folder=tpch-sf${sf}

rm -rf *.tbl
rm -rf ${folder}
./dbgen -s ${sf}
echo tables generated

tables=(nation.tbl region.tbl part.tbl supplier.tbl partsupp.tbl customer.tbl orders.tbl lineitem.tbl)

for table in "${tables[@]}"
do
	sed -i '' 's/.$//' ${table}
	echo ${table} formatted
done

sed -i '' '1s/^/N_NATIONKEY|N_NAME|N_REGIONKEY|N_COMMENT\'$'\n/' nation.tbl
sed -i '' '1s/^/R_REGIONKEY|R_NAME|R_COMMENT\'$'\n/' region.tbl
sed -i '' '1s/^/P_PARTKEY|P_NAME|P_MFGR|P_BRAND|P_TYPE|P_SIZE|P_CONTAINER|P_RETAILPRICE|P_COMMENT\'$'\n/' part.tbl
sed -i '' '1s/^/S_SUPPKEY|S_NAME|S_ADDRESS|S_NATIONKEY|S_PHONE|S_ACCTBAL|S_COMMENT\'$'\n/' supplier.tbl
sed -i '' '1s/^/PS_PARTKEY|PS_SUPPKEY|PS_AVAILQTY|PS_SUPPLYCOST|PS_COMMENT\'$'\n/' partsupp.tbl
sed -i '' '1s/^/C_CUSTKEY|C_NAME|C_ADDRESS|C_NATIONKEY|C_PHONE|C_ACCTBAL|C_MKTSEGMENT|C_COMMENT\'$'\n/' customer.tbl
sed -i '' '1s/^/O_ORDERKEY|O_CUSTKEY|O_ORDERSTATUS|O_TOTALPRICE|O_ORDERDATE|O_ORDERPRIORITY|O_CLERK|O_SHIPPRIORITY|O_COMMENT\'$'\n/' orders.tbl
sed -i '' '1s/^/L_ORDERKEY|L_PARTKEY|L_SUPPKEY|L_LINENUMBER|L_QUANTITY|L_EXTENDEDPRICE|L_DISCOUNT|L_TAX|L_RETURNFLAG|L_LINESTATUS|L_SHIPDATE|L_COMMITDATE|L_RECEIPTDATE|L_SHIPINSTRUCT|L_SHIPMODE|L_COMMENT\'$'\n/' lineitem.tbl

mkdir ${folder}
mv *.tbl ${folder}
