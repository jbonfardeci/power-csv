$APP_ROOT = $env:USERPROFILE
$folder = "$APP_ROOT\OneDrive\Desktop\PowerCSV"
Import-Module -Force "$folder\PowerCSV.psm1"

function getSqlConnectionString([string]$server, [string]$dbname, [bool]$isTrusted=$false, [string]$username=$null, [string]$password=$null){
    if(-not $server -or -not $dbname){
        write-host "Missing required server/DB name fields for SQL Connection string!" -ForegroundColor Red
        return $null
    }

    $cs = "Data Source=$server; Initial Catalog=$dbName; Trusted_Connection=True;"

    # Use username/pwd if not trusted connection.
    if(-not $isTrusted){
        if(-not $username -or -not $password){
            write-host "Missing required username/password credentials for SQL Connection string!" -ForegroundColor Red
            return $null
        }
        $cs = "Data Source=$server; Initial Catalog=$dbName; User Id=$username; Password=$password; Trusted_Connection=False; Encrypt=True;"
    }

    return $cs
}

$callback = [ScriptBlock]({
    param($powerCsv);
    $status = [string]$powerCsv.Status
    $perc = [decimal][math]::Round($powerCsv.PercentComplete, 2)
    Write-Progress -PercentComplete $perc -Activity $status
})

$server = "localhost"
$dbname = "DevTest"
$filepath = "$APP_ROOT\source\repos\data-sci-notebooks\data\diabetes.csv"
$tablename = "dbo.Diabetes"
$cs = (getSqlConnectionString $server $dbname $true)

$csv = (Get-PowerCSV $filepath $tablename $cs)
$csv.Callback = $callback
$csv.HeaderRowCount = 1
$csv.Delimiter = ","
$csv.BatchSize = 1000
$csv.TruncateTable = $true
$csv.Verbose = $true
$rowCount = $csv.TotalRowCount

$rowsWritten = $csv.ImportCsvToDatabase()

if($csv.Error){
    write-host "CSV Import Error: $($csv.Error)" -ForegroundColor Red
}

if($rowCount -eq $rowsWritten){
    write-host "$rowCount of $rowsWritten rows were imported successfully." -ForegroundColor Green
}