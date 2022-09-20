# download data from s3 on all storage nodes

# import util
this_dir=$(dirname "$0")
util_param_path=$(dirname "$this_dir")"/util_param.sh"
source "$util_param_path"
util_func_path=$(dirname "$this_dir")"/util_func.sh"
source "$util_func_path"

# configurable parameters
data_relative_dirs=("tpch-sf10")

# add node ip to ssh
for node_ip in "${fpdb_store_ips[@]}"
do
  check_or_add_to_known_hosts "$node_ip"
done

# download data on each storage node in parallel
pids=()
for node_ip in "${fpdb_store_ips[@]}"
do
  run_command "$pem_path" "$node_ip" "$this_dir""/prepare_data_from_s3.sh" "${data_relative_dirs[@]}" "&"
  pids[${#pids[@]}]=$!
done

# wait
for pid in "${pids[@]}"
do
    wait $pid
done
