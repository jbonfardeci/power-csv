

class PowerCSV{

    #region Constructor
    PowerCSV(
        [string]$pathOrContent, 
        [string]$tableName, 
        [string]$connectionString, 
        [string]$delimiter=",", 
        [int]$headerRowCount=1, 
        [string]$columnNames=$null, 
        [int]$batchSize=1000, 
        [int]$timeOut=300)
    {
        [bool]$isPath = ($pathOrContent -match '^[A-Za-z]\:(\\|/)' -and [System.IO.File]::Exists($pathOrContent));

        if($isPath){
            $this.Path = $pathOrContent;
        }
        else{
            $this.Content = $pathOrContent;
        }
        $this.TableName = $tableName;
        $this.ConnectionString = $connectionString;
        $this.Delimiter = $delimiter;
        $this.HeaderRowCount = $headerRowCount;
        $this.ColumnNames = $columnNames;
        $this.BatchSize = $batchSize;
        $this.TimeOut = $timeOut;
    }
    #endregion Constructor

    #region Public properties
    [string]$Path="";
    [string]$Content="";
    [string]$TableName="";
    [string]$ConnectionString="";
    [string]$Delimiter=",";
    [int]$HeaderRowCount=1;
    [string]$ColumnNames=$null;
    [int]$BatchSize=1000;
    [long]$RowsWritten=0;
    [int]$BatchCount=0;
    [int]$TimeOut=300;
    [long]$TotalRowCount=0;
    [bool]$TruncateTable=$false;
    [bool]$Verbose=$false;
    [string]$Error=$null;
    [long]$MaxRows=0;
    [ScriptBlock]$Callback=$null;
    [string]$Status=$null;
    [decimal]$PercentComplete=0;
    #endregion Public properties

    #region Private properties

    [System.Data.DataTable]$__batchDataTable = $null;

    #endregion Private properties

    #region Public methods

    [long]GetTotalRowCount(){
        if($this.Verbose){
            $this._log("Getting total row count...");
        }
        $skip = $this.HeaderRowCount;
        $rc = 0
        if($this.Path){
            [long]$rc = [long]0;
            $reader = [System.IO.File]::OpenText($this.Path);
            while($reader.ReadLine()){
                $rc++;
            }
            $reader.Dispose();
        }
        elseif($this.Content -and $this.Content.GetType() -eq 'System.Object[]'){
            $rc = [long]$this.Content.Count;
        }

        $rc -= $skip;
        if($this.Verbose){
            $this._log("Total row count = $($rc).");
        }
        $this.TotalRowCount = $rc;
        return $rc;
    }

    static [System.Data.DataTable]GetSqlDataTable([string]$sql, [string]$sqlConnectionString, [int]$timeout=300){
        $cmd = [System.Data.SqlClient.SqlCommand]::new();
        $cmd.Connection = [System.Data.SqlClient.SqlConnection]::new($sqlConnectionString);
        $cmd.CommandTimeout = $timeout;
        $cmd.CommandText = $sql;
        $cmd.CommandType = [System.Data.CommandType]::Text;
        $da = [System.Data.SqlClient.SqlDataAdapter]::new($cmd);
        $dt = [System.Data.DataTable]::new();
        try{
            $cmd.Connection.Open();
            $da.Fill($dt);
            $cmd.Connection.Close();
        }
        catch [System.Exception]{
            Write-Warning $_;
        }
        finally{
            if($cmd.Connection.State -eq [System.Data.ConnectionState]::Open){
                $cmd.Connection.Close();
            }
            $da.Dispose();
            $cmd.Dispose();
        }
        
        return $dt;
    }

    [System.Data.DataTable]CsvToDataTable([int]$take=25){
        if($take -lt 1){
            $take = 1;
        }
        $take += $this.HeaderRowCount;
        $dt = [System.Data.DataTable]::new()
        $skip = $this.HeaderRowCount;
        $contents = $this._getCsvContents(0, $take);
        $headers = $contents | Select-Object -First $skip;
        $columns = $this._createHeaderFromCsv($headers);
        $dupColCount = 1;
        foreach($name in $columns){
            $name = $this._cleanColumnName($name);
            if($dt.Columns.Contains($name)){
                $dupColCount++;
                $name = "$($name)_$($dupColCount)";
            }
            $dc = [System.Data.DataColumn]::new($name);
            $dc.DataType = "".GetType();
            $dt.Columns.Add($dc);
        }

        $this._fillDataTable($dt, ($contents | Select-Object -First $take -Skip $skip));
        return $dt;
    }

    [System.Data.DataTable]CsvToSqlTableSchema([int]$take=25){

        if(-not $this.TableName){
            return $this.CsvToDataTable($take);
        }

        $dt = [System.Data.DataTable]$this._createDataTableFromSqlSchema();
        $skip = $this.HeaderRowCount;
        $contents = $this._getCsvContents($skip, $take);
        $this._fillDataTable($dt, $contents);
        return $dt;
    }

    [long]ImportCsvToDatabase(){
        $this.__resetError();
        [int]$_headerRowCount = $this.HeaderRowCount;
        [int]$_batchCount = 0;
        [int]$_batchSize = $this.BatchSize;
        [long]$_lineCount = 0;
        [long]$_rowsWritten = 0;
        $_dataRows = New-Object System.Collections.Generic.List[string];

        if($this.TruncateTable){
            $this._truncateTable();
        }

        try{
            [long]$totalDataRows = 0;
            
            if(-not $this.MaxRows -or $this.MaxRows -eq 0){
                $totalDataRows = $this.GetTotalRowCount();
            }
            else{
                $totalDataRows = $this.MaxRows; 
            }

            [long]$totalBatches = $totalDataRows / $_batchSize;
            [long]$remainder = $totalDataRows % $_batchSize;

            if($this.Verbose){
                $this._log("
                Total data rows: $totalDataRows
                Batch size: $_batchSize
                Total batches: $totalBatches
                Remainder: $remainder");
            }

            foreach($line in [System.IO.File]::ReadLines($this.Path)){
                $_lineCount++;

                # Skip header row(s) if it has one.
                if($_lineCount -le $_headerRowCount){
                    continue;
                }

                $_dataRows.Add($line);

                # Write data to database if we reach the batch size.
                [long]$remaining = ($totalDataRows - ($_batchSize*$_batchCount));

                if(
                    ($totalDataRows -lt $_batchSize -and $_dataRows.Count -eq $totalDataRows) -or
                    ($_batchCount -lt $totalBatches -and $_dataRows.Count -eq $_batchSize) -or
                    ($remaining -lt $_batchSize -and $_dataRows.Count -eq $remainder)
                ){
                    $_batchCount++;
                    $_rowsWritten += $this._importBatchToDatabase($_dataRows);
                    $this.RowsWritten += $_rowsWritten;

                    $perc = $_rowsWritten/$totalDataRows*100;
                    $this.PercentComplete = $perc;
                    $this.Status = "Remaining: $($remaining). Written: $($_rowsWritten). Percent Complete: $([math]::Round($perc,2))%";

                    if($this.Callback){
                        $params = @($this);
                        $this.Callback.Invoke($params);
                    }

                    if($this.Verbose){
                        $this._log($this.Status);
                    }
                }      
            }
        }
        catch [System.Exception]{
            $this.__setError($_);
            throw [Exception]::new($_);
        }
        finally{
            $_dataRows.Clear();
            if($this.__batchDataTable){
                $this.__batchDataTable.Dispose();
            }
            $this.RowsWritten = $_rowsWritten;
            $this.BatchCount = $_batchCount;
        }

        if($this.Verbose){
            $this._log("Rows written: $($_rowsWritten).");
        }
        return $_rowsWritten;
    }

    #endregion Public methods

    #region Private methods

    [void]__setError($e){
        $this.Error = $e.ToString();
    }

    [void]__resetError(){
        $this.Error = $null;
    }

    [void]_log([string]$msg){
        if($this.Messenger -and $this.Messenger.GetType() -match 'ScriptBlock$'){
            (& $this.Messenger $msg);
        }
        else{
            write-host $msg -ForegroundColor Cyan;
        }
    }

    [int]_truncateTable(){       
        if($this.Verbose){
            $this._log("Truncating table: $($this.TableName)...");
        }

        $sql = "TRUNCATE TABLE $($this.TableName)";
        $cs = $this.ConnectionString;
        $cmd = [System.Data.SqlClient.SqlCommand]::new();
        $cmd.CommandType = [System.Data.CommandType]::Text;
        $cmd.CommandText = $sql;
        $result = $null;
        try{
            $cmd.Connection = [System.Data.SqlClient.SqlConnection]::new($cs);
            $cmd.Connection.Open();
            $result = $cmd.ExecuteNonQuery();
        }
        catch [System.Exception] {
            $this.__setError("Truncate Table Error: $($_)");
        }
        finally{
            if($cmd.Connection.State -eq [System.Data.ConnectionState]::Open){
                $cmd.Connection.Close()
            }
            $cmd.Dispose()
            if($this.Verbose){
                $this._log("Table: $($this.TableName) truncated.");
            }
        }
        return $result
    }

    [System.Collections.Generic.List[string[]]]_createHeaderFromCsv([string[]]$headers){
        
        function replaceEmptyColumnName([string]$col, [int]$ix){
            if($col.Trim().Length -eq 0){
                return "Column_$($ix)";
            }
            return $col.Trim();
        }

        $del = $this.Delimiter;
        $hdr = [System.Collections.Generic.List[string[]]]::new();
        $ct = 1;
        for($i=0; $i -lt $headers.Count; $i++){
            if($i -eq 0){
                $cols = $headers[0].Split($del) | ForEach-Object { $hdr.Add( (replaceEmptyColumnName $_ ($ct++)) )};
            }
            else{
                $cols = $headers[$i].Split($del);
                for($j=0; $j -lt $hdr.Count; $j++){
                    $spacer = "";
                    $col = $cols[$j].Trim();
                    if($col.Length -gt 0){
                        $spacer = "_";
                    }
                    $hdr[$j] = "$($hdr[$j])$($spacer)$($cols[$j].Trim())"
                }
            }
        }
        return $hdr;
    }

    [string[]]_getCsvContents([int]$skip=0, [int]$take=25){
        if($take -lt 1){
            $take = 1;
        }
        $take += $this.HeaderRowCount;

        if($this.Path){
            return Get-Content $this.Path -TotalCount $take | Select-Object -Skip $skip;
        }
        elseif($this.Content){
            return $this.Content | Select-Object -Skip $skip -First $take;
        }
        return $null;
    }

    [string[]]_getCsvColNames(){
        $line = "";
        if($this.ColumnNames){
            $line = $this.ColumnNames;
        }
        elseif($this.Path){
            $line = Get-Content $this.Path -TotalCount 1
        }
        elseif($this.Content -and $this.Content.GetType() -eq 'System.Object[]'){
            $line = $this.Content[0];
        }

        $cols = @();
        $this._getRxCsv().Split($line) | ForEach-Object { $cols += @("[$($_)]") }
        return $cols;
    }

    [bool]_isEmpty([string]$value){
        $value = $this._cleanString($value);
        return [string]::IsNullOrEmpty($value) -or [string]::IsNullOrWhiteSpace($value) -or $value -match '^(\(null\)|null|n\\a)$';
    }

    # [string]_parseNumeric([string]$val){
    #     $val = $this._cleanString($val);

    #     if($this._isEmpty($val)){
    #         return $null;
    #     }

    #     $isPercent = ($val -match '\%$');
    #     $n = ($val -replace '[^0-9\.\-]', '');
    #     [decimal]$d = $null;

    #     if([decimal]::TryParse($n, [ref]$d)){
    #         if($isPercent){
    #             $d = $d/100;
    #         }
    #         return $d.ToString();
    #     }

    #     return $null;
    # }

    [string]_cleanString($val){
        $val = $val -replace '(^\"|\"$|^\s+|\s+$)', '';
        $val = $val -replace '(\r\n|\n)', ' ';
        return $val;
    }

    [string]_cleanColumnName([string]$val){
        if($val -match ('^\d')){
            $val = "_$($val)";
        }
        return $val -replace '[^a-z0-9_]', '_'
    }

    [System.Text.RegularExpressions.Regex]$_rxCsv=$null;
    [System.Text.RegularExpressions.Regex]_getRxCsv(){
        if(-not $this._rxCsv){
            $opt = [System.Text.RegularExpressions.RegexOptions]::Multiline;
            $pattern = $this.Delimiter + '(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))';
            $this._rxCsv = [System.Text.RegularExpressions.Regex]::new($pattern, $opt);
        }
        return $this._rxCsv;
    }

    [System.Data.DataTable]_createDataTableFromSqlSchema(){
        $this.__resetError();
        $tblName = $this.TableName;
        if(-not $tblName){
            return $null;
        }
        $cs = $this.ConnectionString;
        $colNames = [string[]]$this._getCsvColNames();
        $select = $colNames -join ',';
        $cmd = [System.Data.SqlClient.SqlCommand]::new();
        $cmd.Connection = [System.Data.SqlClient.SqlConnection]::new($cs);
        $cmd.CommandType = [System.Data.CommandType]::Text;
        $da = [System.Data.SqlClient.SqlDataAdapter]::new($cmd);
        $dt = [System.Data.DataTable]::new();     
        $cmd.CommandText = "SELECT TOP 1 $select FROM $tblName";

        try{
            $cmd.Connection.Open();
            $da.Fill($dt);
        }
        catch [System.Exception]{
            $this.__setError($_);
        }
        finally{
            if($cmd.Connection.State -eq [System.Data.ConnectionState]::Open){
                $cmd.Connection.Close();
            }
            $da.Dispose();
            $cmd.Dispose();
        }

        $dataTable = $dt.Clone();
        $dt.Dispose();
        return $dataTable;
    }

    [object]_getValue([string]$value, [string]$type){
        $val = $this._cleanString($value);
        
        if($this._isEmpty($val)){
            return [System.DBNull]::Value;
        }
        
        switch($type){
            "System.Int32"{
                return [System.Convert]::ToInt32($val);
            }
            "System.Int64"{
                return [System.Convert]::ToInt64($val);
            }
            "System.Int16" {
                return [System.Convert]::ToInt16($val);
            }
            "System.Double"{
                return [System.Convert]::ToDouble($val);
            }
            "System.Decimal"{
                return [System.Convert]::ToDecimal($val);
            }
            "System.Single"{
                return [System.Convert]::ToSingle($val);
            }
            "System.Boolean"{
                if($val -match '(true|yes|1)'){
                    return $true;
                }
                return $false;
            }
            "System.DateTime"{
                [System.DateTime]$d = [System.DateTime]::Now;
                if([System.DateTime]::TryParse($val, [ref]$d)){
                    return $d;
                }
            }
            "System.Char"{
                [System.Char]$ch = $null;
                if([System.Char]::TryParse($val, [ref]$ch)){
                    return $ch;
                }
            }
            "System.String"{
                return $val;
            }
            default{
                return [System.DBNull]::Value;
            }
        }  
        return [System.DBNull]::Value;
    }

    [void]_fillDataTable([System.Data.DataTable]$dt, [string[]]$lines){
        foreach($line in $lines){
            if($this._isEmpty($line)){
                continue;
            }

            $row = [System.Data.DataRow]$dt.NewRow();
            [string[]]$values = $this._getRxCsv().Split($line);
            for($i=0; $i -lt $dt.Columns.Count; $i++)
            {
                $col = $dt.Columns[$i];
                $colName = $col.ColumnName;
                $type = [string]$col.DataType.FullName;
                $val = [Object]$this._getValue($values[$i], $type);
                $row[$colName] = $val;
            }
            $dt.Rows.Add($row);
        }
    }

    [long]_importBatchToDatabase([System.Collections.Generic.List[string]]$lines){
        $this.__resetError();
        if(-not $this.__batchDataTable){
            $this.__batchDataTable = $this._createDataTableFromSqlSchema();
        }
        $dt = [System.Data.DataTable]$this.__batchDataTable.Clone();
        $this._fillDataTable($dt, $lines);

        [long]$rowCount = 0
        $conn = New-Object System.Data.SqlClient.SqlConnection($this.connectionString)
        $conn.Open();
        $transaction = [System.Data.SqlClient.SqlTransaction]$conn.BeginTransaction();
        $opt = [System.Data.SqlClient.SqlBulkCopyOptions]::Default;
        [System.Data.SqlClient.SqlBulkCopy]$bulkCopy = New-Object System.Data.SqlClient.SqlBulkCopy($conn, $opt, $transaction);
        $bulkCopy.DestinationTableName = $this.TableName;

        try
        {
            $bulkCopy.BulkCopyTimeout = $this.TimeOut;
            foreach ($col in $dt.Columns)
            {
                $bulkCopy.ColumnMappings.Add($col.ColumnName,$col.ColumnName);
            }
            $bulkCopy.WriteToServer($dt);
            $transaction.Commit();
            $rowCount += $dt.Rows.Count;
        }
        catch [System.Exception]{
            $this.__setError($_);
            throw [Exception]::new($_);
        }
        finally
        {
            $bulkCopy.Dispose();
            $transaction.Dispose();
            $conn.Close();
            $dt.Clear()
            $dt.Dispose();
            $lines.Clear();
        }
        return $rowCount;
    }

    #endregion Private methods
}

#region Export Modules

function Get-PowerCSV(
    [string]$pathOrContent, 
    [string]$tableName=$null, 
    [string]$connectionString=$null, 
    [string]$delimiter=",", 
    [int]$headerRowCount=1, 
    [string]$columnNames=$null, 
    [int]$batchSize=1000, 
    [int]$timeOut=300){

    return [PowerCSV]::new($pathOrContent, $tableName, $connectionString, 
        $delimiter, $headerRowCount, $columnNames, $batchSize, $timeOut);
}

function Get-SqlDataTable([string]$sql, [string]$sqlConnectionString, [int]$timeout=300){
    return [PowerCSV]::GetSqlDataTable($sql, $sqlConnectionString, $timeout);
}

Export-ModuleMember Get-PowerCSV;
Export-ModuleMember Get-SqlDataTable;

#endregion Export Modules