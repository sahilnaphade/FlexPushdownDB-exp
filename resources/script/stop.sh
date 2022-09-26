# stop the entire system, both compute and storage (if needed)

# import util
pushd "$(dirname "$0")" > /dev/null
util_param_path=$(pwd)"/util_param.sh"
source "$util_param_path"
util_func_path=$(pwd)"/util_func.sh"
source "$util_func_path"
popd > /dev/null

# storage
if [ "${use_fpdb_store}" = true ]; then
  echo "[Stop FPDB store]"
  ./fpdb-store/stop.sh
fi

# compute
echo "[Stop compute layer]"
./compute/stop.sh
