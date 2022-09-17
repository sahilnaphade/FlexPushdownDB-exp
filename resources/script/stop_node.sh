# stop server on the current node

# import util
util_param_path=$(dirname "$0")"/util_param.sh"
source "$util_param_path"
util_func_path=$(dirname "$0")"/util_func.sh"
source "$util_func_path"

# start server
server_pid_path="$temp_deploy_dir"/"$server_pid_name"
if [ -e "$server_pid_path" ]; then
	kill "$(cat "$server_pid_path")"
	rm "$server_pid_path"
fi
