function RetrieveProcessRiverData($Config)
{
	$ErrorActionPreference = "Stop";

    # BAFU source files and corresponding columns in destination file
	$srcFiles = "BAFU2009_02_00_02","BAFU2009_03_00_02","BAFU2009_04_00_02","BAFU2009_05_00_02","BAFU2009_06_00_02","BAFU2009_07_00_02","BAFU2009_10_10_02"
    $dataCols = "Level [m a.s.l.]","Temperature [°C]","Oxygen [mg/l]","pH [pH]","Conductivity [µS/cm]","Turbidity [TEF]","Flow [m³/s]"	
    # $srcFiles = "BAFU2009_02_00_02","BAFU2009_03_00_02","BAFU2009_04_00_02","BAFU2009_05_00_02","BAFU2009_06_00_02","BAFU2009_10_10_02"
    # $dataCols = "Level [m a.s.l.]","Temperature [°C]","Oxygen [mg/l]","pH [pH]","Conductivity [µS/cm]","Flow [m³/s]"
    # Expected column headers in each source file
    $srcCols = ";Datum","Zeit","Wert","Intervall","Qualität","Messart"
    # Rhone sub-folder
    $rhoneSubFolder = "Porte-du-Scex"

    Write-Host "Downloading BAFU hydrology data..."
    $tempSuffix = "_current.txt"
    $readers = @()
    
	for ($i=0; $i -lt $srcFiles.Length; $i++) {
        FTPGetFile $($srcFiles[$i] + ".txt") $($Config["HydroDataFolder"] + "/" + $rhoneSubFolder + "/" + $srcFiles[$i] + $tempSuffix) $Config["HydroUsername"] $Config["HydroPassword"] $Config["HydroHostname"]
        # directly open the file for reading and read header
        $readers += [System.IO.File]::OpenText($($Config["HydroDataFolder"] + "/" + $rhoneSubFolder + "/" + $srcFiles[$i] + $tempSuffix))
        $readers[$i].ReadLine() |Out-Null # File format/copyright
        $readers[$i].ReadLine() |Out-Null # Station and quantity ID
        $readers[$i].ReadLine() |Out-Null # Human-readable file description
        $headers = $readers[$i].ReadLine().split("`t") | %{ $_.trim() } # Column titles
        if (diff $headers $srcCols) {
            Write-Host "Error: Unexpected column headers in" $srcFiles[$i]
            exit 1
        }
	}

    Write-Host "Parsing data files..."
    $fileOpen = $false
    $destHeader = "Date`tTime`t" + [string]::join("`t",$dataCols)
    $prevDate = 0
    
    try {
        for() {
            $refLine = $readers[0].ReadLine()
            if ($refLine -eq $null) { break }
            $refLine = $refLine.split("`t")
            $refDate = parseBAFUdate $refLine[0] $refLine[1]
            $refDateStr = $refDate.toString("dd.MM.yyyy")
            $refTime = $refDate.toString("HH:mm")

            # Create one file per day, overwriting existing ones
            # - starting at the first time the timestamp hits Midnight
            if (((-not $fileOpen) -and ($refTime -eq "00:00")) -or ($fileOpen -and ($refDateStr -ne $prevDate.toString("dd.MM.yyyy")))) {
                if ($fileOpen) {
                    Write-Host $dest ":" $recCount "lines of data written."
                }
                $YearFolder = $Config["HydroDataFolder"] + "/" + $rhoneSubFolder + "/" + $refDate.Year + "/"
                TestCreatePath $YearFolder
                $dest = $YearFolder + "BAFU2009_" + $refDate.toString("yyyyMMdd") + ".txt"
                $destHeader | Out-File -Encoding utf8 $dest
                $fileOpen = $true
                $recCount = 0
            }
            if (-not $fileOpen) {
                continue
            }

            # Reformat the date and time to get rid of "24:00" timestamps
            $destLine = $refDateStr, $refTime, $refLine[2]

            # Check for irregular sampling times
            if ($refDate.Minute % 10 -gt 0) {
                Write-Host "Warning:" $refDateStr ": Skipping unexpected sampling time" $refTime
                continue
            }

            # Check sampling interval
            if ($prevDate -ne 0) {
                $interval = ($refDate - $prevDate).TotalMinutes
                if($interval -ne 10) {
                    Write-Host "Warning:" $refDateStr $refTime ": Unexpected sampling interval of" $interval "minutes"
                }
            }

            $validLine = "NaN"
            # Now read all the other quantities for this time
            for ($i=1; $i -lt $srcFiles.Length; $i++) {
                do {
                    try {
                        $l = $readers[$i].ReadLine().split("`t")
                    } catch {
                        Write-Host "Warning:" $srcFiles[$i] ": probable empty file, values replaced by NaNs"
                        $l = $validLine
                        $l[2] = "NaN"
                    }
                    $curDate = parseBAFUdate $l[0] $l[1]
                    $validLine = $l
                } while ($curDate -lt $refDate)
                if ($curDate -eq $refDate) {
                    $destLine += $l[2]
                    # Sanity checks
                    if($l[4] -ne 0 -or $l[5] -ne 1){
                        Write-Host "Warning:" $refDateStr $refTime ": Unexpected quality (" $l[4] ") or measurement type (" $l[5] ") in" $srcFiles[$i]
                    }
                } else {
                    Write-Host "Warning:" $refDateStr $refTime ": No data for" $dataCols[$i]
                    $destLine += "NaN" # Missing data point
                }
            }
            $destLine = [string]::join("`t",$destLine)
            $destLine | Out-File -Append -Encoding utf8 $dest
            $recCount++
            $prevDate = $refDate
        }
    }
    finally {
        Write-Host $dest ":" $recCount "lines of data written."
        for ($i=0; $i -lt $srcFiles.Length; $i++) {
            $readers[$i].Close()
            rm $($Config["HydroDataFolder"] + "/" + $rhoneSubFolder + "/" + $srcFiles[$i] + $tempSuffix)
        }
    }



    # Discharge forecast (independent processing)
    $forecastFiles = "Pqprevi_COSMO1_2009","Pqprevi_COSMOE_2009"
    $forecastHeaders = "dd mm yyyy hh Wasserstand Abfluss","dd mm yyyy hh  H_e01   H_e02   H_e03   H_e04   H_e05   H_e06   H_e07   H_e08   H_e09   H_e10   H_e11   H_e12   H_e13   H_e14   H_e15   H_e16   H_e17   H_e18   H_e19   H_e20   Q_e01   Q_e02   Q_e03   Q_e04   Q_e05   Q_e06   Q_e07   Q_e08   Q_e09   Q_e10   Q_e11   Q_e12   Q_e13   Q_e14   Q_e15   Q_e16   Q_e17   Q_e18   Q_e19   Q_e20   H_min   H_p25   H_p50   H_p75   H_max   Q_min   Q_p25   Q_p50   Q_p75   Q_max"

    Write-Host "Downloading and processing forecast files..."
    for ($i=0; $i -lt $forecastFiles.Length; $i++) {
        FTPGetFile $($forecastFiles[$i] + ".txt") $($Config["HydroDataFolder"] + "/" + $rhoneSubFolder + "/" + $forecastFiles[$i] + $tempSuffix) $Config["HydroUsername"] $Config["HydroPassword"] $Config["HydroHostname"]
        $rdr = [System.IO.File]::OpenText($($Config["HydroDataFolder"] + "/" + $rhoneSubFolder + "/" + $forecastFiles[$i] + $tempSuffix))

        try {

            # Ignore all lines before the column header
            do {
                $header = $rdr.ReadLine()
                if($header -eq $null) {
                    Write-Host $forecastFiles[$i] ": Error: Unexpected data format"
                    exit 1
                }
            } while ($header -ne $forecastHeaders[$i])

            # Generate column headers from input headers and write them
            $dest = $YearFolder + "BAFU2009_COSMO" + (IIf ($i -eq 0) "1_" "E_") + $(Get-Date).toString("yyyyMMdd") + ".txt"
            $destHeader = parseSpaceSeparated $forecastHeaders[$i]
            $destHeader = ("Date","Time") + $destHeader[4..($destHeader.length-1)]
            [string]::join("`t",$destHeader) | Out-File -encoding utf8 $dest
            $recCount = 0

            for() {
                $line = $rdr.ReadLine()
                if ($line -eq $null) { break }
                $line = parseSpaceSeparated $line
                $date = $line[0] + "." + $line[1] + "." + $line[2]
                $time = $line[3] + ":00"
                $line = ($date, $time) + $line[4..($line.length-1)]
                [string]::join("`t",$line) | Out-File -Append -Encoding utf8 $dest
                $recCount++
            }
        }
        finally {
            $rdr.close()
            rm $($Config["HydroDataFolder"] + "/" + $rhoneSubFolder + "/" + $forecastFiles[$i] + $tempSuffix)
            Write-Host $dest ":" $recCount "lines of data written."
        }
    }



    # Water levels and outflows (independent processing)
    $waterlevelFiles = "BAFU2027_02_00_02","BAFU2028_02_00_02","BAFU2606_10_35_02","BAFU2209_02_01_02","BAFU2014_02_00_02","BAFU2099_10_11_02"
    $waterlevelOutHeader = "Date`tTime`tLevel [m a.s.l.]","Date`tTime`tLevel [m a.s.l.]","Date`tTime`tFlow [m³/s]","Date`tTime`tLevel [m a.s.l.]","Date`tTime`tLevel [m a.s.l.]","Date`tTime`tFlow [m³/s]"
    # Expected column headers in each source file
    $waterlevelHeader = ";Datum","Zeit","Wert","Intervall","Qualität","Messart"
    $waterlevelSubFolder = "St-Prex","Secheron","Halle-de-l-ile","Zurichsee","Obersee","Limmat"
    $waterlevelOutPrefix = "BAFU2027_","BAFU2028_","BAFU2606_","BAFU2209_","BAFU2014_","BAFU2099_"

    Write-Host "Downloading and parsing waterlevels & outflow files..."
    
    for ($i=0; $i -lt $waterlevelFiles.Length; $i++) {
        
        FTPGetFile $($waterlevelFiles[$i] + ".txt") $($Config["HydroDataFolder"] + "/" + $waterlevelSubFolder[$i] + "/" + $waterlevelFiles[$i] + $tempSuffix) $Config["HydroUsername"] $Config["HydroPassword"] $Config["HydroHostname"]
        
        # directly open the file for reading and read header
        $WLreader = [System.IO.File]::OpenText($($Config["HydroDataFolder"] + "/" + $waterlevelSubFolder[$i] + "/" + $waterlevelFiles[$i] + $tempSuffix))

        $WLreader.ReadLine() |Out-Null # File format/copyright
        $WLreader.ReadLine() |Out-Null # Station and quantity ID
        $WLreader.ReadLine() |Out-Null # Human-readable file description
        $headers = $WLreader.ReadLine().split("`t") | %{ $_.trim() } # Column titles
        if (diff $headers $srcCols) {
            Write-Host "Error: Unexpected column headers in" $srcFiles[$i]
            exit 1
        }

        $fileOpen = $false
        $destHeader = $waterlevelOutHeader[$i]
        $prevDate = 0

        try {
            for() {
                $refLine = $WLreader.ReadLine()
                if ($refLine -eq $null) { break }
                $refLine = $refLine.split("`t")
                $refDate = parseBAFUdate $refLine[0] $refLine[1]
                $refDateStr = $refDate.toString("dd.MM.yyyy")
                $refTime = $refDate.toString("HH:mm")

                # Create one file per day, overwriting existing ones
                # - starting at the first time the timestamp hits Midnight
                if (((-not $fileOpen) -and ($refTime -eq "00:00")) -or ($fileOpen -and ($refDateStr -ne $prevDate.toString("dd.MM.yyyy")))) {
                    if ($fileOpen) {
                        Write-Host $dest ":" $recCount "lines of data written."
                    }
                    $YearFolder = $Config["HydroDataFolder"] + "/" + $waterlevelSubFolder[$i] + "/" + $refDate.Year + "/"
                    TestCreatePath $YearFolder
                    $dest = $YearFolder + $waterlevelOutPrefix[$i] + $refDate.toString("yyyyMMdd") + ".txt"
                    $destHeader | Out-File -Encoding utf8 $dest
                    $fileOpen = $true
                    $recCount = 0
                }
                if (-not $fileOpen) {
                    continue
                }

                # Reformat the date and time to get rid of "24:00" timestamps
                $destLine = $refDateStr, $refTime, $refLine[2]

                # Check for irregular sampling times
                if ($refDate.Minute % 10 -gt 0) {
                    Write-Host "Warning:" $refDateStr ": Skipping unexpected sampling time" $refTime
                    continue
                }

                # Check sampling interval
                if ($prevDate -ne 0) {
                    $interval = ($refDate - $prevDate).TotalMinutes
                    if($interval -ne 10) {
                        Write-Host "Warning:" $refDateStr $refTime ": Unexpected sampling interval of" $interval "minutes"
                    }
                }

                # Now read the quantities for this time
                
                #do {
                #    $l = $WLreader.ReadLine().split("`t")
                #    $curDate = parseBAFUdate $l[0] $l[1]
                #} while ($curDate -lt $refDate)
                #if ($curDate -eq $refDate) {
                #    $destLine += $l[2]
                #    # Sanity checks
                #    if($l[4] -ne 0 -or $l[5] -ne 1){
                #        Write-Host "Warning:" $refDateStr $refTime ": Unexpected quality (" $l[4] ") or measurement type (" $l[5] ") in" $srcFiles[$i]
                #    }
                #} else {
                #    Write-Host "Warning:" $refDateStr $refTime ": No data for" $waterlevelFiles[$i]
                #    $destLine += "NaN" # Missing data point
                #}
                
                $destLine = [string]::join("`t",$destLine)
                $destLine | Out-File -Append -Encoding utf8 $dest
                $recCount++
                $prevDate = $refDate
            }
        }
    
        finally {
            Write-Host $dest ":" $recCount "lines of data written."
            
            $WLreader.Close()
            rm $($Config["HydroDataFolder"] + "/" + $waterlevelSubFolder[$i] + "/" + $waterlevelFiles[$i] + $tempSuffix)
            
        }
    }


    # Independent processing of the Linth (flow + temperature)

    # BAFU source files and corresponding columns in destination file
	$srcFiles = "BAFU2104_03_00_02","BAFU2104_10_10_02"
    $dataCols = "Temperature [°C]","Flow [m³/s]"	
    # Expected column headers in each source file
    $srcCols = ";Datum","Zeit","Wert","Intervall","Qualität","Messart"
    # Rhone sub-folder
    $rhoneSubFolder = "Linth"

    Write-Host "Downloading Linth river data..."
    $tempSuffix = "_current.txt"
    $readers = @()
    
	for ($i=0; $i -lt $srcFiles.Length; $i++) {
        FTPGetFile $($srcFiles[$i] + ".txt") $($Config["HydroDataFolder"] + "/" + $rhoneSubFolder + "/" + $srcFiles[$i] + $tempSuffix) $Config["HydroUsername"] $Config["HydroPassword"] $Config["HydroHostname"]
        # directly open the file for reading and read header
        $readers += [System.IO.File]::OpenText($($Config["HydroDataFolder"] + "/" + $rhoneSubFolder + "/" + $srcFiles[$i] + $tempSuffix))
        $readers[$i].ReadLine() |Out-Null # File format/copyright
        $readers[$i].ReadLine() |Out-Null # Station and quantity ID
        $readers[$i].ReadLine() |Out-Null # Human-readable file description
        $headers = $readers[$i].ReadLine().split("`t") | %{ $_.trim() } # Column titles
        if (diff $headers $srcCols) {
            Write-Host "Error: Unexpected column headers in" $srcFiles[$i]
            exit 1
        }
	}

    Write-Host "Parsing data files..."
    $fileOpen = $false
    $destHeader = "Date`tTime`t" + [string]::join("`t",$dataCols)
    $prevDate = 0
    
    try {
        for() {
            $refLine = $readers[0].ReadLine()
            if ($refLine -eq $null) { break }
            $refLine = $refLine.split("`t")
            $refDate = parseBAFUdate $refLine[0] $refLine[1]
            $refDateStr = $refDate.toString("dd.MM.yyyy")
            $refTime = $refDate.toString("HH:mm")

            # Create one file per day, overwriting existing ones
            # - starting at the first time the timestamp hits Midnight
            if (((-not $fileOpen) -and ($refTime -eq "00:00")) -or ($fileOpen -and ($refDateStr -ne $prevDate.toString("dd.MM.yyyy")))) {
                if ($fileOpen) {
                    Write-Host $dest ":" $recCount "lines of data written."
                }
                $YearFolder = $Config["HydroDataFolder"] + "/" + $rhoneSubFolder + "/" + $refDate.Year + "/"
                TestCreatePath $YearFolder
                $dest = $YearFolder + "BAFU2104_" + $refDate.toString("yyyyMMdd") + ".txt"
                $destHeader | Out-File -Encoding utf8 $dest
                $fileOpen = $true
                $recCount = 0
            }
            if (-not $fileOpen) {
                continue
            }

            # Reformat the date and time to get rid of "24:00" timestamps
            $destLine = $refDateStr, $refTime, $refLine[2]

            # Check for irregular sampling times
            if ($refDate.Minute % 10 -gt 0) {
                Write-Host "Warning:" $refDateStr ": Skipping unexpected sampling time" $refTime
                continue
            }

            # Check sampling interval
            if ($prevDate -ne 0) {
                $interval = ($refDate - $prevDate).TotalMinutes
                if($interval -ne 10) {
                    Write-Host "Warning:" $refDateStr $refTime ": Unexpected sampling interval of" $interval "minutes"
                }
            }

            $validLine = "NaN"
            # Now read all the other quantities for this time
            for ($i=1; $i -lt $srcFiles.Length; $i++) {
                do {
                    try {
                        $l = $readers[$i].ReadLine().split("`t")
                    } catch {
                        Write-Host "Warning:" $srcFiles[$i] ": probable empty file, values replaced by NaNs"
                        $l = $validLine
                        $l[2] = "NaN"
                    }
                    $curDate = parseBAFUdate $l[0] $l[1]
                    $validLine = $l
                } while ($curDate -lt $refDate)
                if ($curDate -eq $refDate) {
                    $destLine += $l[2]
                    # Sanity checks
                    if($l[4] -ne 0 -or $l[5] -ne 1){
                        Write-Host "Warning:" $refDateStr $refTime ": Unexpected quality (" $l[4] ") or measurement type (" $l[5] ") in" $srcFiles[$i]
                    }
                } else {
                    Write-Host "Warning:" $refDateStr $refTime ": No data for" $dataCols[$i]
                    $destLine += "NaN" # Missing data point
                }
            }
            $destLine = [string]::join("`t",$destLine)
            $destLine | Out-File -Append -Encoding utf8 $dest
            $recCount++
            $prevDate = $refDate
        }
    }
    finally {
        Write-Host $dest ":" $recCount "lines of data written."
        for ($i=0; $i -lt $srcFiles.Length; $i++) {
            $readers[$i].Close()
            rm $($Config["HydroDataFolder"] + "/" + $rhoneSubFolder + "/" + $srcFiles[$i] + $tempSuffix)
        }
    }


        # Discharge forecast (independent processing)
    $forecastFiles = "Pqprevi_COSMO1_2104","Pqprevi_COSMOE_2104"
    $forecastHeaders = "dd mm yyyy hh Wasserstand Abfluss","dd mm yyyy hh  H_e01   H_e02   H_e03   H_e04   H_e05   H_e06   H_e07   H_e08   H_e09   H_e10   H_e11   H_e12   H_e13   H_e14   H_e15   H_e16   H_e17   H_e18   H_e19   H_e20   Q_e01   Q_e02   Q_e03   Q_e04   Q_e05   Q_e06   Q_e07   Q_e08   Q_e09   Q_e10   Q_e11   Q_e12   Q_e13   Q_e14   Q_e15   Q_e16   Q_e17   Q_e18   Q_e19   Q_e20   H_min   H_p25   H_p50   H_p75   H_max   Q_min   Q_p25   Q_p50   Q_p75   Q_max"

    Write-Host "Downloading and processing forecast files..."
    for ($i=0; $i -lt $forecastFiles.Length; $i++) {
        FTPGetFile $($forecastFiles[$i] + ".txt") $($Config["HydroDataFolder"] + "/" + $rhoneSubFolder + "/" + $forecastFiles[$i] + $tempSuffix) $Config["HydroUsername"] $Config["HydroPassword"] $Config["HydroHostname"]
        $rdr = [System.IO.File]::OpenText($($Config["HydroDataFolder"] + "/" + $rhoneSubFolder + "/" + $forecastFiles[$i] + $tempSuffix))

        try {

            # Ignore all lines before the column header
            do {
                $header = $rdr.ReadLine()
                if($header -eq $null) {
                    Write-Host $forecastFiles[$i] ": Error: Unexpected data format"
                    exit 1
                }
            } while ($header -ne $forecastHeaders[$i])

            # Generate column headers from input headers and write them
            $dest = $YearFolder + "BAFU2104_COSMO" + (IIf ($i -eq 0) "1_" "E_") + $(Get-Date).toString("yyyyMMdd") + ".txt"
            $destHeader = parseSpaceSeparated $forecastHeaders[$i]
            $destHeader = ("Date","Time") + $destHeader[4..($destHeader.length-1)]
            [string]::join("`t",$destHeader) | Out-File -encoding utf8 $dest
            $recCount = 0

            for() {
                $line = $rdr.ReadLine()
                if ($line -eq $null) { break }
                $line = parseSpaceSeparated $line
                $date = $line[0] + "." + $line[1] + "." + $line[2]
                $time = $line[3] + ":00"
                $line = ($date, $time) + $line[4..($line.length-1)]
                [string]::join("`t",$line) | Out-File -Append -Encoding utf8 $dest
                $recCount++
            }
        }
        finally {
            $rdr.close()
            rm $($Config["HydroDataFolder"] + "/" + $rhoneSubFolder + "/" + $forecastFiles[$i] + $tempSuffix)
            Write-Host $dest ":" $recCount "lines of data written."
        }
    }


    
    Write-Host "File parsing completed."

}


# Convert data line separated by multiple spaces to an array
function parseSpaceSeparated($line) {
    do {
        $l = $line
        $line = $l.replace("  "," ")
    } while($line -ne $l)
    return $line.split(" ")
}


# Convert BAFU date and time to a date-time object
function parseBAFUdate($date, $time) {
    if ($time -eq "24:00"){
        return $(Get-Date $date).AddDays(1)
    } else {
        return Get-Date $($date+" "+$time)
    }
}


# Inline If (http://stackoverflow.com/questions/25682507/powershell-inline-if-iif)
function IIf($If, $IfTrue, $IfFalse) {
    If ($If -IsNot "Boolean") {$_ = $If}
    If ($If) {If ($IfTrue -is "ScriptBlock") {&$IfTrue} Else {$IfTrue}}
    Else {If ($IfFalse -is "ScriptBlock") {&$IfFalse} Else {$IfFalse}}
}