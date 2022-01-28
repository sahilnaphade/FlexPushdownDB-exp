# start the system on all cluster nodes

# parameters
deploy_dir_name="FPDB-build"
pem_path="$HOME""/.aws/yifei-aws-wisc.pem"

# get script path and import
pushd "$(dirname "$0")" > /dev/null
script_dir=$(pwd)
source "$script_dir""/util.sh"

## directories
#deploy_dir=$HOME/$deploy_dir_name
#resource_dir="$(dirname "${script_dir}")"
#
## 1. master ip and slave ips
#master_ip="$(curl -s ifconfig.me)"
#
#cluster_ips_path="${resource_dir}""/config/cluster_ips"
#while IFS= read -r line || [[ -n "$line" ]];
#do
#  if [ "$line" != "$master_ip" ]; then
#    slave_ips+=("$line")
#  fi
#done < "$cluster_ips_path"

run_command "$pem_path" "18.191.79.246" /home/ubuntu/start.sh

# back to original directory
popd > /dev/null
