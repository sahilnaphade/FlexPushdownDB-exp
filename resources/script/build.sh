# script to build the system locally

# import util
util_path=$(dirname "$0")"/util.sh"
source "$util_path"

pushd "$(dirname "$0")" > /dev/null

# make build directory
if [ "${clean}" = true ]; then
  rm -rf "${build_dir}"
fi
mkdir -p "${build_dir}"

# load cmake
if [ "$(uname)" = "Darwin" ]; then
  # mac OS
  C_compiler="/usr/local/opt/llvm@13/bin/clang-13"
  CXX_compiler="/usr/local/opt/llvm@13/bin/clang++"
elif [ "$(expr substr "$(uname -s)" 1 5)" == "Linux" ]; then
  # linux
  C_compiler="/usr/bin/clang-12"
  CXX_compiler="/usr/bin/clang++-12"
else
  echo "*** Error: supported OS are Mac OS or Linux."
  exit 0
fi

cd "${build_dir}"
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER="${C_compiler}" -DCMAKE_CXX_COMPILER="${CXX_compiler}" \
-G "CodeBlocks - Unix Makefiles" "${root_dir}"

# use exec.conf.ec2 for calcite
calcite_config_dir="$root_dir"/"$calcite_dir_name""/main/resources/config"
mv "$calcite_config_dir""/exec.conf" "$calcite_config_dir""/exec.conf.backup"
cp "$calcite_config_dir""/exec.conf.ec2" "$calcite_config_dir""/exec.conf"

# build targets
for target in "${targets[@]}"
do
  cmake --build . --target "${target}" -- -j "${build_parallel}"
done

# restore exec.conf
rm "$calcite_config_dir""/exec.conf"
mv "$calcite_config_dir""/exec.conf.backup" "$calcite_config_dir""/exec.conf"

popd > /dev/null