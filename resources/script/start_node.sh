# start server on the current node

# import util
util_path=$(dirname "$0")"/util.sh"
source "$util_path"

# export lib paths to LD_LIBRARY_PATH
lib_deploy_dir="$deploy_dir""/libs"
export LD_LIBRARY_PATH=${lib_deploy_dir}/aws-cpp-sdk_ep/install/lib:${lib_deploy_dir}/caf_ep/install/lib:\
${lib_deploy_dir}/graphviz_ep/install/lib:${lib_deploy_dir}/graphviz_ep/install/lib/graphviz

# start server
target_name="normal-frontend-server"
target_path="$deploy_dir"/"$exe_dir_name"/"$target_name"
server_pid_path="$deploy_dir"/"$exe_dir_name"/"$server_pid_name"
nohup "$target_path" >/dev/null &
echo $! > "$server_pid_path"
