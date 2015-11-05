### Licensed to the Apache Software Foundation (ASF) under one or more
### contributor license agreements.  See the NOTICE file distributed with
### this work for additional information regarding copyright ownership.
### The ASF licenses this file to You under the Apache License, Version 2.0
### (the "License"); you may not use this file except in compliance with
### the License.  You may obtain a copy of the License at
###
###     http://www.apache.org/licenses/LICENSE-2.0
###
### Unless required by applicable law or agreed to in writing, software
### distributed under the License is distributed on an "AS IS" BASIS,
### WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
### See the License for the specific language governing permissions and
### limitations under the License.

###
### A set of basic PowerShell routines that can be used to install and
### manage Hadoop services on a single node. For use-case see install.ps1.

###
### Global variables
###
$ScriptDir = Resolve-Path (Split-Path $MyInvocation.MyCommand.Path)
$FinalName = "hbase-@version@"
$DefaultRoles = @("master","regionserver","hbrest","thrift","thrift2")
$WaitingTime = 10000

###############################################################################
###
### Installs HBase.
###
### Arguments:
###     component: Component to be installed, it can be "hbase".
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     role: Space separated list of roles that should be installed.
###           (for example, "master regionserver" for mapreduce)
###
###############################################################################
function Install(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=2, Mandatory=$true )]
    $serviceCredential,
    [String]
    [Parameter( Position=3, Mandatory=$false )]
    $roles
    )
{
    if ( $component -eq "hbase" )
    {
        $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"

        ### $HbaseInstallDir: the directory that contains the application, after unzipping
        $hbaseInstallToDir = Join-Path "$nodeInstallRoot" "$FinalName"
        $hbaseInstallToBin = Join-Path "$hbaseInstallToDir" "bin"
        Write-Log "hbaseInstallToDir: $hbaseInstallToDir"
		Write-Log "hbaseInstallToBin: $hbaseInstallToBin"
        
        Write-Log "Installing Apache HBase $FinalName to $hbaseInstallToDir"

        ### Create Node Install Root directory
        if( -not (Test-Path "$hbaseInstallToDir"))
        {
            Write-Log "Creating Node Install Root directory: `"$hbaseInstallToDir`""
            $cmd = "mkdir `"$hbaseInstallToDir`""
            Invoke-CmdChk $cmd
        }        
        if ($roles ) {
            CheckRole $roles $DefaultRoles
        }
        Write-Log "Checking the HBase Installation before copying the HBase bits"
        if( -not (Test-Path $ENV:HBASE_HOME\bin\hbase.cmd))
        {
            InstallBinaries $nodeInstallRoot $serviceCredential
        }
        
        Write-Log "Adding the HBase custom jars into HBASE_CLASSPATH if exists"
        if ((Test-Path ENV:COMPONENT_RESOURCES_LOCATION) -and (Test-Path $ENV:COMPONENT_RESOURCES_LOCATION ))
        {
            $hbaseResourceDir = Join-Path "$ENV:COMPONENT_RESOURCES_LOCATION" "HBase\*"
            [Environment]::SetEnvironmentVariable( "HBASE_CLASSPATH", $ENV:HBASE_CLASSPATH + ";" + $hbaseResourceDir, [EnvironmentVariableTarget]::Machine )
            $ENV:HBASE_CLASSPATH = $ENV:HBASE_CLASSPATH + ";" + $hbaseResourceDir
        }

        ###
        ### Create HBase Windows Services and grant user ACLS to start/stop
        ###

        Write-Log "Node HBase Role Services: $roles"
        $allServices = $roles

        Write-Log "Installing services $allServices"

        foreach( $service in empty-null $allServices.Split(' '))
        {
            CreateAndConfigureHadoopService $service $HDP_RESOURCES_DIR $hbaseInstallToBin $serviceCredential

            Write-Log "Creating service config ${baseInstallToBin}\$service.xml"
            
            ###
            ### Select the appropriate service
            ###
            switch ($service)
            {
                master 
                {
                    $cmd = "$hbaseInstallToBin\hbase.cmd --service master start > `"$hbaseInstallToBin`"\master.xml"
                    break
                }
                regionserver
                {
                    $cmd = "$hbaseInstallToBin\hbase.cmd --service regionserver start > `"$hbaseInstallToBin`"\regionserver.xml"
                    break
                }
                hbrest
                {
                    $cmd = "$hbaseInstallToBin\hbase.cmd --service rest start > `"$hbaseInstallToBin`"\hbrest.xml"
                    break
                }
                thrift
                {
                    $cmd = "$hbaseInstallToBin\hbase.cmd --service thrift start > `"$hbaseInstallToBin`"\thrift.xml"
                    break
                }
                thrift2
                {
                    $cmd = "$hbaseInstallToBin\hbase.cmd --service thrift2 start > `"$hbaseInstallToBin`"\thrift2.xml"
                    break
                }
                default
                {
                    WriteLog "Service role "$service" is unexpected."
                    $cmd = ""
                    break
                }
            }
            
            ###
            ### Execute the command if the command string is empty.
            ###
            if (![string]::IsNullorEmpty($cmd))
            {
                Invoke-CmdChk $cmd
            }
        }

        ### Configure the default log locations
        $hbaselogsdir = "$hbaseInstallToDir\logs"
        if (Test-Path ENV:HBASE_LOG_DIR)
        {
            $hbaselogsdir = "$ENV:HBASE_LOG_DIR"
        }
        Configure "hbase" $nodeInstallRoot $serviceCredential @{
        "hbase.log.dir" = "$hbaselogsdir"}
    }
    else
    {
        throw "Install: Unsupported component argument."
    }
}

###############################################################################
###
### Uninstalls Hadoop component.
###
### Arguments:
###     component: Component to be uninstalled, it can be "core, "hdfs" or "mapreduce"
###     nodeInstallRoot: Install folder (for example "C:\Hadoop")
###
###############################################################################
function Uninstall(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot
    )
{
    if ( $component -eq "hbase" )
    {

        $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"

        ### $hbaseInstallDir: the directory that contains the appliation, after unzipping
        $hbaseInstallToDir = Join-Path "$nodeInstallRoot" "$FinalName"
        Write-Log "hbaseInstallToDir: $hbaseInstallToDir"

        ###
        ### Stop and delete HBase services
        ###
        foreach( $service in $DefaultRoles)
        {
            StopAndDeleteHadoopService $service
        }

        ###
        ### Delete the HBase directory
        ###
        Write-Log "Deleting $hbaseInstallToDir"
        $cmd = "rd /s /q `"$hbaseInstallToDir`""
        Invoke-Cmd $cmd

        ###
        ### Removing HBASE installation environment variables
        ###
        Write-Log "Removing HBASE_HOME, and HBASE_CONF_DIR environment variables at machine scope"
        [Environment]::SetEnvironmentVariable( "HBASE_HOME", $null, [EnvironmentVariableTarget]::Machine )
        [Environment]::SetEnvironmentVariable( "HBASE_CONF_DIR", $null, [EnvironmentVariableTarget]::Machine )
    }
    else
    {
        throw "Uninstall: Unsupported compoment argument."
    }
}

###############################################################################
###
### Alters the configuration of the component.
###
### Arguments:
###     component: Component to be configured, e.g "hbase"
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     configs: Configuration that should be applied.
###              For example, @{"fs.checkpoint.edits.dir" = "C:\Hadoop\hdfs\2nne"}
###     aclAllFolders: If true, all folders defined in config file will be ACLed
###                    If false, only the folders listed in $configs will be ACLed.
###
###############################################################################
function Configure(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=2, Mandatory=$true )]
    $serviceCredential,
    [hashtable]
    [parameter( Position=3 )]
    $configs = @{},
    [bool]
    [parameter( Position=4 )]
    $aclAllFolders = $True
    )
{
    if ( $component -eq "hbase" )
    {
        $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"

        ### $hbaseInstallDir: the directory that contains the application, after unzipping
        $hbaseInstallToDir = Join-Path "$nodeInstallRoot" "$FinalName"
        $hbaseInstallToBin = Join-Path "$hbaseInstallToDir" "bin"
        Write-Log "hbaseInstallToDir: $hbaseInstallToDir"

        if( -not (Test-Path $hbaseInstallToDir ))
        {
            throw "ConfigureHBase: Install the hbase before configuring it"
        }
        
        ###
        ### Apply configuration changes to service xmls
        ###
        foreach( $service in ("hbrest", "regionserver", "master", "thrift", "thrift2"))
        {
            $serviceXmlFile = Join-Path $hbaseInstallToDir "bin\$service.xml"
            # Apply configs only if the service xml exist
            if ( Test-Path $serviceXmlFile )
            {
                [hashtable]$configs = ConfigureServiceXml $serviceXmlFile $service $configs
            }
        }

        ###
        ### Apply configuration changes to environment variables
        ###
        [hashtable]$configs = UpdateEnvVariables $configs

        ###
        ### Apply configuration changes to hbase-site.xml
        ###
        $xmlFile = Join-Path $hbaseInstallToDir "conf\hbase-site.xml"
        UpdateXmlConfig $xmlFile $configs
    }
    else
    {
        throw "Configure: Unsupported compoment argument."
    }
}

###############################################################################
###
### Start component services.
###
### Arguments:
###     component: Component name
###     roles: List of space separated service to start
###
###############################################################################
function StartService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $roles
    )
{
    Write-Log "Starting `"$component`" `"$roles`" services"

    if ( $component -eq "hbase" )
    {
        ### Verify that roles are in the supported set
        CheckRole $roles $DefaultRoles

        foreach ( $role in $roles.Split(" ") )
        {
            if ( $role -ne "thrift" -and $role -ne "thrift2" )
            {
                Write-Log "Starting $role service"
                Start-Service $role
            }
        }
    }
    else
    {
        throw "StartService: Unsupported component argument."
    }
}

###############################################################################
###
### Stop component services.
###
### Arguments:
###     component: Component name
###     roles: List of space separated service to stop
###
###############################################################################
function StopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $roles
    )
{
    Write-Log "Stopping `"$component`" `"$roles`" services"

    if ( $component -eq "hbase" )
    {
        ### Verify that roles are in the supported set
        CheckRole $roles $DefaultRoles

        foreach ( $role in $roles.Split(" ") )
        {
            try
            {
                Write-Log "Stopping $role "
                if (Get-Service "$role" -ErrorAction SilentlyContinue)
                {
                    Write-Log "Service $role exists, stopping it"
                    Stop-Service $role
                }
                else
                {
                    Write-Log "Service $role does not exist, moving to next"
                }
            }
            catch [Exception]
            {
                Write-Host "Can't stop service $role"
            }
        }
    }
    else
    {
        throw "StartService: Unsupported compoment argument."
    }
}

###############################################################################
###
### Installs HBase binaries.
###
### Arguments:
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###
###############################################################################
function InstallBinaries(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=2, Mandatory=$true )]
    $serviceCredential
    )
{
    $username = $serviceCredential.UserName

    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"

    ### $hbaseInstallDir: the directory that contains the application, after unzipping
    $hbaseInstallToDir = Join-Path "$nodeInstallRoot" "$FinalName"
    $hbaseLogsDir = Join-Path "$hbaseInstallToDir" "logs"
    if (Test-Path ENV:HBASE_LOG_DIR)
    {
        $hbaseLogsDir = "$ENV:HBASE_LOG_DIR"
    }
    Write-Log "hbaseLogsDir: $hbaseLogsDir"


    Write-Log "Checking the JAVA Installation."
    if( -not (Test-Path $ENV:JAVA_HOME\bin\java.exe))
    {
      Write-Log "JAVA_HOME not set properly; $ENV:JAVA_HOME\bin\java.exe does not exist" "Failure"
      throw "Install: JAVA_HOME not set properly; $ENV:JAVA_HOME\bin\java.exe does not exist."
    }

    Write-Log "Checking the Hadoop Installation."
    if( -not (Test-Path $ENV:HADOOP_HOME\bin\winutils.exe))
    {
      Write-Log "HADOOP_HOME not set properly; $ENV:HADOOP_HOME\bin\winutils.exe does not exist" "Failure"
      throw "Install: HADOOP_HOME not set properly; $ENV:HADOOP_HOME\bin\winutils.exe does not exist."
    }

    ###
    ### Set HBASE_HOME environment variable
    ###
    Write-Log "Setting the HBASE_HOME environment variable at machine scope to `"$hbaseInstallToDir`""
    [Environment]::SetEnvironmentVariable("HBASE_HOME", $hbaseInstallToDir, [EnvironmentVariableTarget]::Machine)
    $ENV:HBASE_HOME = $hbaseInstallToDir

    ### HBase Binaries must be installed before creating the services
    ###
    ### Begin install
    ###
    Write-Log "Installing Apache HBase $FinalName to $nodeInstallRoot"

    ### Create Node Install Root directory
    if( -not (Test-Path "$nodeInstallRoot"))
    {
        Write-Log "Creating Node Install Root directory: `"$nodeInstallRoot`""
        New-Item -Path "$nodeInstallRoot" -type directory | Out-Null
    }

    ###
    ###  Unzip Hadoop distribution from compressed archive
    ###
    Write-Log "Extracting HBase archive into $hbaseInstallToDir"
    if ( Test-Path ENV:UNZIP_CMD )
    {
        ### Use external unzip command if given
        $unzipExpr = $ENV:UNZIP_CMD.Replace("@SRC", "`"$HDP_RESOURCES_DIR\$FinalName.zip`"")
        $unzipExpr = $unzipExpr.Replace("@DEST", "`"$nodeInstallRoot`"")
        ### We ignore the error code of the unzip command for now to be
        ### consistent with prior behavior.
        Invoke-PsChk $unzipExpr
    }
    else
    {
        $shellApplication = new-object -com shell.application
        $zipPackage = $shellApplication.NameSpace("$HDP_RESOURCES_DIR\$FinalName.zip")
        $destinationFolder = $shellApplication.NameSpace($nodeInstallRoot)
        $destinationFolder.CopyHere($zipPackage.Items(), 20)
    }

    ###
    ###  Copy template configuration files
    ###
    Write-Log "Copying template files"
    $xcopy_cmd = "xcopy /EIYF `"$HDP_INSTALL_PATH\..\template`" `"$hbaseInstallToDir`""
    Invoke-Cmd $xcopy_cmd

    ###
    ### Grant Hadoop user access to $hbaseInstallToDir
    ###
    GiveFullPermissions $hbaseInstallToDir $username

    ###
    ### ACL HBase logs directory such that machine users can write to it
    ###
    if( -not (Test-Path "$hbaseLogsDir"))
    {
        Write-Log "Creating HBase logs folder"
        New-Item -Path "$hbaseLogsDir" -type directory | Out-Null
    }

    GiveFullPermissions "$hbaseLogsDir" "*S-1-5-32-545"

    [Environment]::SetEnvironmentVariable( "HBASE_CONF_DIR", "$ENV:HBASE_HOME\conf", [EnvironmentVariableTarget]::Machine )

    Write-Log "Installation of Apache HBase binaries completed"
}


### Helper routing that converts a $null object to nothing. Otherwise, iterating over
### a $null object with foreach results in a loop with one $null element.
function empty-null($obj)
{
   if ($obj -ne $null) { $obj }
}

### Gives full permissions on the folder to the given user
function GiveFullPermissions(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $folder,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $username)
{
    Write-Log "Giving user/group `"$username`" full permissions to `"$folder`""
    $cmd = "icacls `"$folder`" /grant ${username}:(OI)(CI)F"
    Invoke-CmdChk $cmd
}

### Checks if the given space separated roles are in the given array of
### supported roles.
function CheckRole(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $roles,
    [array]
    [parameter( Position=1, Mandatory=$true )]
    $supportedRoles
    )
{
    foreach ( $role in $roles.Split(" ") )
    {
        if ( -not ( $supportedRoles -contains $role ) )
        {
            throw "CheckRole: Passed in role `"$role`" is outside of the supported set `"$supportedRoles`""
        }
    }
}

### Creates and configures the service.
function CreateAndConfigureHadoopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $service,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $hdpResourcesDir,
    [String]
    [Parameter( Position=2, Mandatory=$true )]
    $serviceBinDir,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=3, Mandatory=$true )]
    $serviceCredential
)
{
    if ( -not ( Get-Service "$service" -ErrorAction SilentlyContinue ) )
    {
        Write-Log "Creating service `"$service`" as $serviceBinDir\$service.exe"
        $xcopyServiceHost_cmd = "copy /Y `"$hdpResourcesDir\serviceHost.exe`" `"$serviceBinDir\$service.exe`""
        Invoke-CmdChk $xcopyServiceHost_cmd

        #HadoopServiceHost.exe will write to this log but does not create it
        #Creating the event log needs to be done from an elevated process, so we do it here
        if( -not ([Diagnostics.EventLog]::SourceExists( "$service" )))
        {
            [Diagnostics.EventLog]::CreateEventSource( "$service", "" )
        }

        Write-Log "Adding service $service"
        $s = New-Service -Name "$service" -BinaryPathName "$serviceBinDir\$service.exe" -Credential $serviceCredential -DisplayName "Apache Hadoop $service"
        if ( $s -eq $null )
        {
            throw "CreateAndConfigureHadoopService: Service `"$service`" creation failed"
        }

        $cmd="$ENV:WINDIR\system32\sc.exe failure $service reset= 30 actions= restart/5000"
        Invoke-CmdChk $cmd

        $cmd="$ENV:WINDIR\system32\sc.exe config $service start= demand"
        Invoke-CmdChk $cmd

        Set-ServiceAcl $service
    }
    else
    {
        Write-Log "Service `"$service`" already exists, Removing `"$service`""
        StopAndDeleteHadoopService $service
        CreateAndConfigureHadoopService $service $hdpResourcesDir $serviceBinDir $serviceCredential
    }
}

### Forces a service to stop
function ForceStopService(
    [ServiceProcess.ServiceController]
    [Parameter( Position=0, Mandatory=$true )]
    $s
)
{
    Stop-Service -InputObject $s -Force
    $ServiceProc = Get-Process -Id (Get-WmiObject win32_Service | Where {$_.Name -eq $s.Name}).ProcessId -ErrorAction SilentlyContinue
    if( $ServiceProc.Id -ne 0 )
    {
        if( $ServiceProc.WaitForExit($WaitingTime) -eq $false )
        {
            Write-Log "Process $ServiceProc cannot be stopped. Trying to kill the process"
            Stop-Process $ServiceProc -Force  -ErrorAction Continue
        }
     }
}

### Stops and deletes the Hadoop service.
function StopAndDeleteHadoopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $service
)
{
    Write-Log "Stopping $service"
    $s = Get-Service $service -ErrorAction SilentlyContinue
    if( $s -ne $null )
    {
        try
        {
            ForceStopService $s
        }
        catch
        {
            Write-Log "ForceStopService: Failed with exception: $($_.Exception.ToString())"
        }
        $cmd = "sc.exe delete $service"
        Invoke-Cmd $cmd
    }
}

### Helper routine that updates the given fileName XML file with the given
### key/value configuration values. The XML file is expected to be in the
### Hadoop format. For example:
### <configuration>
###   <property>
###     <name.../><value.../>
###   </property>
### </configuration>
function UpdateXmlConfig(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $fileName,
    [hashtable]
    [parameter( Position=1 )]
    $config = @{} )
{
    $xml = New-Object System.Xml.XmlDocument
    $xml.PreserveWhitespace = $true
    $xml.Load($fileName)

    foreach( $key in empty-null $config.Keys )
    {
        $value = $config[$key]
        $found = $False
        $xml.SelectNodes('/configuration/property') | ? { $_.name -eq $key } | % { $_.value = $value; $found = $True }
        if ( -not $found )
        {
            $xml["configuration"].AppendChild($xml.CreateWhitespace("`r`n  ")) | Out-Null
            $newItem = $xml.CreateElement("property")
            $newItem.AppendChild($xml.CreateWhitespace("`r`n    ")) | Out-Null
            $newItem.AppendChild($xml.CreateElement("name")) | Out-Null
            $newItem.AppendChild($xml.CreateWhitespace("`r`n    ")) | Out-Null
            $newItem.AppendChild($xml.CreateElement("value")) | Out-Null
            $newItem.AppendChild($xml.CreateWhitespace("`r`n  ")) | Out-Null
            $newItem.name = $key
            $newItem.value = $value
            $xml["configuration"].AppendChild($newItem) | Out-Null
            $xml["configuration"].AppendChild($xml.CreateWhitespace("`r`n")) | Out-Null
        }
    }

    $xml.Save($fileName)
    $xml.ReleasePath
}

### Helper routine that updates the given fileName XML file with the given
### configuration values. The XML file is expected to be in the service
### XML format. For example:
### <service>
###   <arguments>
###     -Xms1024m -server -Xmx2048m -Dhadoop.root.logger=INFO
###   </arguments>
### </service>
###
### Example config hashmap entries:
###   key: Xms
###   value: -Xms1024m
function UpdateServiceXmlConfig(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $fileName,
    [hashtable]
    [parameter( Position=1 )]
    $config = @{} )
{
    $xml = New-Object System.Xml.XmlDocument
    $xml.PreserveWhitespace = $true
    $xml.Load($fileName)

    [string]$value = $xml.service.arguments

    foreach( $key in empty-null $config.Keys )
    {
        # Given how java cmd line args are defined, the only thing
        # we can do at this point is match as prefix. If this turns
        # out to be buggy, we should introduce strongly named configs
        # which are configurable.

        $newArg = $config[$key]
        $escapedKey = [regex]::escape($key)
        # Regex that matches config arg with the following properties
        #  - It is on the beggining of a line or prefixed with space (quoted or unquoted)
        #  - It is on the end of a line or followed with a space (quoted or unquoted)
        #  - it contains the given key followed by: 0-9a-zA-Z\p{P}=
        $regex = "(^| |(^| )"")-$escapedKey[0-9a-zA-Z\p{P}=]*?($| |""(^| ))"

        if ( $value -imatch $regex )
        {
            $value = $value -ireplace $regex, " $newArg "
        }
        else
        {
            # add a new entry to the beginning
            $value = $config[$key] + " " + $value
        }
    }

    $xml.service.arguments = $value

    $xml.Save($fileName)
    $xml.ReleasePath
}

### Helper method that extracts all servicexml configs from the "config"
### hashmap and applies them to corresponding service.xml.
### The function returns the list of configs that are not specific to this
### service.
### Example config:
###   azure.servicexml.datanode.Xms = -Xms256m
### Note that Xms and Xmx configs are case sensitive.
function ConfigureServiceXml(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $serviceXmlFileName,
    [string]
    [parameter( Position=1, Mandatory=$true )]
    $serviceName,
    [hashtable]
    [parameter( Position=2 )]
    $config = @{} )
{
    [hashtable]$newConfig = @{}
    [hashtable]$newServiceConfig = @{}
    foreach( $key in empty-null $config.Keys )
    {
        [string]$keyString = $key
        $value = $config[$key]

        if ( $keyString.StartsWith("azure.servicexml.$serviceName.", "InvariantCultureIgnoreCase") )
        {
            # remove the prefix to align with UpdateServiceXmlConfig contract
            $newKey = $key -ireplace "azure\.servicexml\.$serviceName\.", ""
            $newServiceConfig.Add($newKey, $value) > $null
        }
        else
        {
            $newConfig.Add($key, $value) > $null
        }
    }

    # skip update if there are no service config changes
    if ($newServiceConfig.Count -gt 0)
    {
        UpdateServiceXmlConfig $serviceXmlFileName $newServiceConfig > $null
    }

    $newConfig
}

### Helper method that extracts all environment variables from the 
### "config" hashmap and sets them or replaces the value of the 
### existing ones. The function returns the list of configs that 
### are not including these ENV configs.
### Example config:
###   azure.env.HBASE.HEAPSIZE = 4000
### This equals to setting HBASE_HEAPSIZE = 4000
function UpdateEnvVariables(
    [hashtable]
    [parameter( Position=0 )]
    $config = @{} )
{
    [hashtable]$newConfig = @{}
    foreach( $key in empty-null $config.Keys )
    {
        [string]$keyString = $key
        $value = $config[$key]
        if ( $keyString.StartsWith("azure.env.", "InvariantCultureIgnoreCase") )
        {
            # remove the prefix to get the key
            $newKey = $key -ireplace "azure\.env\.", ""
            [Environment]::SetEnvironmentVariable( $newKey, $value, [EnvironmentVariableTarget]::Machine )
        }
        elseif ( $keyString.StartsWith("azure.servicexml.", "InvariantCultureIgnoreCase") )
        {
            # do not add service configurations into newConfig
        }
        else
        {
            $newConfig.Add($key, $value) > $null
        }
    }

    $newConfig
}

###
### Public API
###
Export-ModuleMember -Function Install
Export-ModuleMember -Function Uninstall
Export-ModuleMember -Function Configure
Export-ModuleMember -Function StartService
Export-ModuleMember -Function StopService
