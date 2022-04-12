$APP_ROOT = $env:USERPROFILE
$folder = "$APP_ROOT\source\repos\power-csv"
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
    $err = $powerCsv.$Error
    if($err){
        Write-Error $err
    }
    else{
        Write-Progress -PercentComplete $perc -Activity $status
    }
})

$server = ".\"
$dbname = "DevTest"
$filepath = "$APP_ROOT\source\repos\power-csv\test\Anscombe.csv"
$tablename = "dbo.Anscombe"
$cs = (getSqlConnectionString $server $dbname $true)

$csv = (Get-PowerCSV $filepath $tablename $cs)
$csv.Callback = $callback
$csv.HeaderRowCount = 1
$csv.Delimiter = "," # If using a pipe ("|") or any other char that is also a regular expression, escape it with a backslash "\". e.g., "\|"
$csv.BatchSize = 1000
$csv.TruncateTable = $true
$csv.Verbose = $true
$rowCount = $csv.TotalRowCount

#$dt = [System.Data.DataTable]$csv.CsvToDataTable()
#write-host $dt.Rows.Count -ForegroundColor Green

$rowsWritten = $csv.ImportCsvToDatabase()

if($csv.Error){
    write-host "CSV Import Error: $($csv.Error)" -ForegroundColor Red
}

if($rowCount -eq $rowsWritten){
    write-host "$rowCount of $rowsWritten rows were imported successfully." -ForegroundColor Green
}