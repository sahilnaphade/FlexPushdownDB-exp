# script to prepare data in storage nodes, when a new instance is started
# including mounting the nvme-ssd storage and downloading data from S3
# need aws credentials already set

# input parameters
data_relative_dirs=("$@")       # can take multiple dirs, e.g. ("tpch-sf0.01" "tpch-sf10)

# configurable parameters
bucket="flexpushdowndb"
mount_point="/fpdb-store"
storage_name="/dev/nvme1n1"
temp_dir="$HOME""/""temp"

# mount nvme-ssd to the mount point and set access
sudo umount "$storage_name"
sudo mkdir -p "$mount_point"

sudo mkfs.ext4 -E nodiscard "$storage_name"
sudo mount "$storage_name" "$mount_point"

bucket_dir="$mount_point""/""$bucket"
sudo mkdir -p "$bucket_dir"
sudo chown -R ubuntu:ubuntu "$mount_point"

# download data from S3
for data_relative_dir in "${data_relative_dirs[@]}"
do
  aws s3 cp --recursive "s3://""$bucket""/""$data_relative_dir" "$temp_dir""/""$data_relative_dir"
  mkdir -p "$(dirname "$bucket_dir""/""$data_relative_dir")"
  mv "$temp_dir""/""$data_relative_dir" "$bucket_dir""/""$data_relative_dir"
done

sudo rm -rf "$temp_dir"
