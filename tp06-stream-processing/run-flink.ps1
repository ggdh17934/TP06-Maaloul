$ErrorActionPreference = 'Stop'
$connector = 'flink-sql-connector-kafka-3.4.0-1.20.jar'
$cache = Join-Path $PSScriptRoot '.flink'
$jar = Join-Path $cache $connector
New-Item -ItemType Directory -Path $cache -Force | Out-Null
if (-not (Test-Path -LiteralPath $jar)) {
    $uri = "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/$connector"
    Invoke-WebRequest -Uri $uri -OutFile "$jar.part"
    Move-Item -LiteralPath "$jar.part" -Destination $jar -Force
}

$start = 'sed -i "s/1728m/1152m/;s/1600m/800m/" /opt/flink/conf/config.yaml; /opt/flink/bin/start-cluster.sh && exec /opt/flink/bin/sql-client.sh -f /lab/flink_window.sql'
docker run --rm --init --name tp06-flink --network container:tp05-kafka `
    --mount "type=bind,source=$jar,target=/opt/flink/lib/flink-sql-connector-kafka.jar,readonly" `
    --mount "type=bind,source=$PSScriptRoot,target=/lab,readonly" `
    --entrypoint bash flink:1.20.2-scala_2.12-java11 -c $start
exit $LASTEXITCODE
