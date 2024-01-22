#!/bin/sh

if test -z "$BASH_SOURCE"
then
      ABSOLUTE_PATH="$(cd "$(dirname "${BASH_SOURCE:-$0}")" && pwd)/$(basename "${BASH_SOURCE:-$0}")"
else
      ABSOLUTE_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
fi
CURRENT_FOLDER="${ABSOLUTE_PATH%/*}"
echo "ABSOLUTEPATH=${ABSOLUTE_PATH}"
echo "CURRENT_FOLDER=${CURRENT_FOLDER}"
TARGET_FOLDER="$CURRENT_FOLDER"/target
echo TARGET_FOLDER=${TARGET_FOLDER}
mkdir -p $TARGET_FOLDER

echo "Building Docker Image"
docker build -t taco-builder $CURRENT_FOLDER --progress=plain --no-cache

echo "Assembling Tableau Connector"
docker run -d -it --name=taco-builder --mount type=bind,source=$TARGET_FOLDER,target=/output taco-builder
echo "Copying Tableau Connector"
docker exec taco-builder sh -c "cp /tableau-sdk/connector-plugin-sdk/connector-packager/packaged-connector/neptune-jdbc-v3.0.2.taco  /output"
echo "Verifying Tableau Connector"
docker exec taco-builder sh -c "ls -l /output"
docker exec taco-builder pwd
echo "Getting Logs"
docker cp taco-builder:/tableau-sdk/connector-plugin-sdk/connector-packager/packaging_logs.txt $TARGET_FOLDER
echo "Extracting Tableau Connector"
docker cp taco-builder:/output/neptune-jdbc-v3.0.2.taco $TARGET_FOLDER
echo "Checking Logs and Resulting TACO FILE in $TARGET_FOLDER"
ls -l $TARGET_FOLDER
docker stop taco-builder
docker rm taco-builder
