param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$Script,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ScriptArguments
)

$ErrorActionPreference = 'Stop'
$python = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw 'Crea prima .venv e installa le dipendenze come indicato nel README.'
}

if (-not $env:JAVA_HOME) {
    $env:JAVA_HOME = 'C:\Program Files\Java\jdk-17'
}
if (-not (Test-Path -LiteralPath (Join-Path $env:JAVA_HOME 'bin\java.exe'))) {
    throw 'Imposta JAVA_HOME sulla cartella del JDK 17, 21 o 25.'
}

if (-not $env:SPARK_HOME) {
    $env:SPARK_HOME = Join-Path $env:USERPROFILE 'Downloads\spark-4.3.0-bin-hadoop3\spark-4.3.0-bin-hadoop3'
}
$submit = Join-Path $env:SPARK_HOME 'bin\spark-submit.cmd'
if (-not (Test-Path -LiteralPath $submit -PathType Leaf)) {
    throw 'Imposta SPARK_HOME sulla cartella che contiene bin\spark-submit.cmd.'
}

if (-not $env:HADOOP_HOME) {
    $env:HADOOP_HOME = Join-Path $PSScriptRoot '.hadoop'
}
$hadoopBin = Join-Path $env:HADOOP_HOME 'bin'
foreach ($native in @('hadoop.dll', 'winutils.exe')) {
    if (-not (Test-Path -LiteralPath (Join-Path $hadoopBin $native) -PathType Leaf)) {
        throw 'Esegui configura_hadoop.py oppure imposta HADOOP_HOME su una distribuzione Hadoop Windows compatibile.'
    }
}
$env:Path = $hadoopBin + ';' + $env:Path

$env:PYSPARK_DRIVER_PYTHON = $python
$env:PYSPARK_PYTHON = $python
& $submit --master 'local[2]' (Join-Path $PSScriptRoot $Script) @ScriptArguments
exit $LASTEXITCODE


