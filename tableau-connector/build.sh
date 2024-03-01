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

DRIVER_VERSION=$1
if [ -z "$DRIVER_VERSION" ]
then
    file="../gradle.properties"
    MAJOR_VERSION=$(grep "MAJOR_VERSION" ${file} | cut -d'=' -f2)
    MINOR_VERSION=$(grep "MINOR_VERSION" ${file} | cut -d'=' -f2)
    PATCH_VERSION=$(grep "PATCH_VERSION" ${file} | cut -d'=' -f2)
    DRIVER_VERSION=$MAJOR_VERSION.$MINOR_VERSION.$PATCH_VERSION
fi
echo DRIVER_VERSION=$DRIVER_VERSION
TACO_NAME="neptune-jdbc-v${DRIVER_VERSION}.taco"
echo TACO_NAME=${TACO_NAME}

echo "Building Docker Image"
docker build -t taco-builder $CURRENT_FOLDER --progress=plain --no-cache

echo "Assembling Tableau Connector"
docker run -d -it --name=taco-builder --mount type=bind,source=$TARGET_FOLDER,target=/output taco-builder
echo "Copying Tableau Connector"
docker exec taco-builder sh -c "cp /tableau-sdk/connector-plugin-sdk/connector-packager/packaged-connector/$TACO_NAME /output"
echo "Verifying Tableau Connector"
docker exec taco-builder sh -c "ls -l /output"
docker exec taco-builder pwd
echo "Getting Logs"
docker cp taco-builder:/tableau-sdk/connector-plugin-sdk/connector-packager/packaging_logs.txt $TARGET_FOLDER
echo "Extracting Tableau Connector"
docker cp taco-builder:/output/$TACO_NAME $TARGET_FOLDER
echo "Checking Logs and Resulting TACO FILE in $TARGET_FOLDER"
ls -l $TARGET_FOLDER
docker stop taco-builder
docker rm taco-builder
