# Script to spread built package to all cluster nodes
# Note: set resources/config/cluster_ips first

trap ctrl_c INT
function ctrl_c() {
  echo "*** Trapped CTRL-C, exit"
  popd > /dev/null
  exit 0
}

# parameters
build_dir_name="cmake-build-release"
deploy_dir_name="FPDB-build"
exe_dir_name="normal-frontend"
calcite_dir_name="normal-calcite/java"
pem_path="$HOME""/.aws/yifei-aws-wisc.pem"

# get script path
pushd "$(dirname "$0")" > /dev/null
script_dir=$(pwd)

# directories
deploy_dir=$HOME/$deploy_dir_name
resource_dir="$(dirname "${script_dir}")"
root_dir="$(dirname "${resource_dir}")"
build_dir="${root_dir}"/"${build_dir_name}"

# 1. local ip and slave ips
local_ip="$(curl -s ifconfig.me)"

cluster_ips_path="${resource_dir}""/config/cluster_ips"
while IFS= read -r line || [[ -n "$line" ]];
do
  if [ $line != $local_ip ]; then
    slave_ips+=("$line")
  fi
done < "$cluster_ips_path"

# 2. organize executables, resources and required libraries
mkdir -p "$deploy_dir"

# executables
exe_dir="$build_dir"/"$exe_dir_name"
exe_deploy_dir="$deploy_dir"/"$exe_dir_name"
cp -r "$exe_dir" "$exe_deploy_dir"

# calcite
calcite_dir="$root_dir"/"$calcite_dir_name"
calcite_deploy_dir="$deploy_dir"/"$calcite_dir_name"
mkdir -p "$(dirname "${calcite_deploy_dir}")"
cp -r "$calcite_dir" "$calcite_deploy_dir"

# use exec.conf.ec2 for calcite
calcite_deploy_config_dir="$calcite_deploy_dir""/main/resources/config"
rm "$calcite_deploy_config_dir""/exec.conf"
cp "$calcite_deploy_config_dir""/exec.conf.ec2" "$calcite_deploy_config_dir""/exec.conf"

# resources
resource_deploy_dir="$deploy_dir""/resources/"
mkdir -p "$(dirname "${resource_deploy_dir}")"
cp -r "$resource_dir" "$resource_deploy_dir"

# libs
lib_names=("aws-cpp-sdk_ep" "caf_ep" "graphviz_ep")
lib_suffix="install/lib"
lib_root_dir="$build_dir""/_deps"
lib_deploy_root_dir="$deploy_dir""/libs"

for lib_name in "${lib_names[@]}"
do
  lib_dir="$lib_root_dir"/"$lib_name"/"$lib_suffix"
  lib_deploy_dir="$lib_deploy_root_dir"/"$lib_name"/"$lib_suffix"
  mkdir -p "$(dirname "${lib_deploy_dir}")"
  cp -r "$lib_dir" "$lib_deploy_dir"
done

# 3. deploy organized package for each node
for slave_ip in "${slave_ips[@]}"
do
  scp -i -r "$pem_path" "$deploy_dir" ubuntu@"$slave_ip":"$deploy_dir"
done

# back to original directory
popd > /dev/null
