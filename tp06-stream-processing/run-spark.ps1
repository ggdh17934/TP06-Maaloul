# Docker Desktop's Linux Spark shares the TP05 broker's localhost listener.
$ErrorActionPreference = 'Stop'
$kafkaPackage = 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3'
$project = $PSScriptRoot
New-Item -ItemType Directory -Path (Join-Path $project '.ivy2') -Force | Out-Null
$containerOptions = @()
if ($env:TP06_SPARK_CONTAINER) {
    $containerOptions = @('--name', $env:TP06_SPARK_CONTAINER)
}
docker run --rm --init @containerOptions --network container:tp05-kafka `
    --mount "type=bind,source=$project,target=/lab" `
    --mount "type=bind,source=$project/.ivy2,target=/tmp/tp06-ivy" `
    --workdir /lab apache/spark:3.5.3-python3 `
    /opt/spark/bin/spark-submit --master 'local[2]' `
    --conf spark.ui.showConsoleProgress=false `
    --conf spark.ui.enabled=false `
    --conf spark.jars.ivy=/tmp/tp06-ivy `
    --conf spark.jars.ivySettings=/lab/ivysettings.xml `
    --packages $kafkaPackage /lab/spark_stream.py @args
exit $LASTEXITCODE
