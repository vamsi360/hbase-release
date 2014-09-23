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

param(
	[String]
    [Parameter( ParameterSetName='UsernamePassword', Position=0, Mandatory=$true )]
    [Parameter( ParameterSetName='UsernamePasswordBase64', Position=0, Mandatory=$true )]
    $username,
    [String]
    [Parameter( ParameterSetName='UsernamePassword', Position=1, Mandatory=$true )]
    $password,
    [String]
    [Parameter( ParameterSetName='UsernamePasswordBase64', Position=1, Mandatory=$true )]
    $passwordBase64,
    [Parameter( ParameterSetName='CredentialFilePath', Mandatory=$true )]
    $credentialFilePath,
	$hbaseRole
	)

function Main( $scriptDir )
{
	$HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "hbase-@version@.winpkg.log"
Test-JavaHome

	### $hbaseInstallDir: the directory that contains the appliation, after unzipping
	$hbaseInstallDir = Join-Path "$ENV:HADOOP_NODE_INSTALL_ROOT" "hbase-@version@"
	$hbaseInstallBin = Join-Path "$hbaseInstallDir" "bin"

	Write-Log "HbaseInstallDir: $hbaseInstallDir"
	Write-Log "HbaseInstallBin: $hbaseInstallBin"

	###
	### Create the Credential object from the given username and password or the provided credentials file
	###
    $serviceCredential = Get-HadoopUserCredentials -credentialsHash @{"username" = $username; "password" = $password; `
        "passwordBase64" = $passwordBase64; "credentialFilePath" = $credentialFilePath}
    $username = $serviceCredential.UserName
	Write-Log "Username: $username"
	Write-Log "CredentialFilePath: $credentialFilePath"

	Write-Log "Ensuring elevated user"

	$currentPrincipal = New-Object Security.Principal.WindowsPrincipal( [Security.Principal.WindowsIdentity]::GetCurrent( ) )
	if ( -not ($currentPrincipal.IsInRole( [Security.Principal.WindowsBuiltInRole]::Administrator ) ) )
	{
		Write-Log "install script must be run elevated" "Failure"
		exit 1
	}

	if( -not (Test-Path $ENV:JAVA_HOME\bin\java.exe))
	{
		Write-Log "JAVA_HOME not set properly; $ENV:JAVA_HOME\bin\java.exe does not exist" "Failure"
		exit 1
	}

	###
	### Begin install
	###
	Write-Log "Installing Apache Hadoop Hbase hbase-@version@ to $ENV:HADOOP_NODE_INSTALL_ROOT"

	if( $username -eq $null )
	{
		Write-Log "Username cannot be empty" "Failure"
		exit 1
	}
        $hbaseRole = " "
	if( $ENV:IS_HBASE_MASTER -eq "yes")
	{
		$hbaseRole =  "master"
	}

	if( $ENV:IS_HBASE_REGIONSERVER -eq "yes" )
	{
		$hbaseRole = $hbaseRole+" "+"regionserver"
	}

	if( $ENV:IS_HBASE_MASTER -eq "yes" -or $ENV:IS_HBASE_REGIONSERVER -eq "yes")
	{
		$hbaseRole = $hbaseRole+" "+"rest"
		$hbaseRole = $hbaseRole+" "+"thrift"
		$hbaseRole = $hbaseRole+" "+"thrift2"
  }

  $hbaseRole = $hbaseRole.Trim()

	###
	###  Unzip Hbase distribution from compressed archive
	###

	Write-Log "Extracting Hbase archive into $hbaseInstallDir"
	$unzipExpr = "$ENV:WINPKG_BIN\winpkg.ps1 `"$HDP_RESOURCES_DIR\hbase-@version@.zip`" utils unzip `"$ENV:HADOOP_NODE_INSTALL_ROOT`""
	Invoke-Ps $unzipExpr

	$xcopy_cmd = "xcopy /EIYF `"$HDP_INSTALL_PATH\..\template`" `"$hbaseInstallDir`""
	Invoke-Cmd $xcopy_cmd

	###
	### Grant Hbase user access to HADOOP_INSTALL_DIR and HDFS Root
	###
	$cmd = "icacls `"$hbaseInstallDir`" /grant `"${username}`":(OI)(CI)F"
	Invoke-Cmd $cmd

	###
	### Create Hbase Windows Services and grant user ACLS to start/stop
	###
    if ($hbaseRole){
        Write-Log "HbaseRole: $hbaseRole"

        Write-Log "Installing services $hbaseRole"

        foreach( $service in $hbaseRole -Split('\s+'))
        {
            try
            {
                Write-Log "Creating service $service as $hbaseInstallBin\$service.exe"
                CreateAndConfigureHadoopService $service $HDP_RESOURCES_DIR $hbaseInstallBin $serviceCredential
				$disabled_services = 'rest','thrift','thrift2'
				if ($disabled_services -match $service)
				{
					$cmd="$ENV:WINDIR\system32\sc.exe config $service start= disabled"
				}
                else
				{
					$cmd="$ENV:WINDIR\system32\sc.exe config $service start= demand"
				}
                Invoke-Cmd $cmd
            }
            catch [Exception]
            {
                Write-Log $_.Exception.Message $_
            }
        ###
        ### Setup HBASE service config
        ###

            Write-Log "Copying configuration for $hbaseRole"

            Write-Log "Creating service config ${hbaseInstallBin}\${service}.xml"
            $cmd = "$hbaseInstallBin\hbase.cmd --service $service start > `"$hbaseInstallBin\$service.xml`""
            Invoke-Cmd $cmd
        }
    }
    else{
        Write-Log "No Hbase roles are defined. Not adding any services"
    }
	###
	### ACL Hbase logs directory such that machine users can write to it
	###
	if( -not (Test-Path "$hbaseInstallDir\logs"))
	{
		Write-Log "Creating Hbase logs folder"
		$cmd = "mkdir `"$hbaseInstallDir\logs`""
		Invoke-Cmd $cmd
	}
	Write-Log "Giving Users group full permissions on the Hbase logs folder"
	$cmd = "icacls `"$hbaseInstallDir\logs`" /grant Users:(OI)(CI)F"
	Invoke-Cmd $cmd

        ###
        ### Apply core-site.xml configuration changes
        ###
        $hbaseSiteXmlFile = Join-Path $hbaseInstallDir "conf\hbase-site.xml"

        Write-Log "Updating hbase config file $hbaseSiteXmlFile"

        $hbaseRootDir = "hdfs://${ENV:NAMENODE_HOST}:8020/apps/hbase/data"

        if ($ENV:HA -ieq "yes") {
            $hbaseRootDir = "hdfs://${ENV:HA_CLUSTER_NAME}/apps/hbase/data"
        }

        UpdateXmlConfig $hbaseSiteXmlFile @{
        "hbase.rootdir" = "$hbaseRootDir";
        "hbase.zookeeper.quorum" = "$ENV:ZOOKEEPER_HOSTS";
        "hbase.zookeeper.property.clientPort" = "2181"}
        if ($ENV:IS_PHOENIX -ieq "yes") {
            UpdateXmlConfig $hbaseSiteXmlFile @{"hbase.regionserver.wal.codec" = "org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec"}
        }


	Write-Log "Installation of Hbase Core complete"
}
### Helper routing that converts a $null object to nothing. Otherwise, iterating over
### a $null object with foreach results in a loop with one $null element.
function empty-null($obj)
{
   if ($obj -ne $null) { $obj }
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
    $xml = [xml] (Get-Content $fileName)

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
        $xcopyServiceHost_cmd = "copy /Y `"$HDP_RESOURCES_DIR\serviceHost.exe`" `"$serviceBinDir\$service.exe`""
        Invoke-CmdChk $xcopyServiceHost_cmd
		
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

        $cmd="$ENV:WINDIR\system32\sc.exe config $service start= disabled"
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
        Stop-Service $service
        $cmd = "sc.exe delete $service"
        Invoke-Cmd $cmd
    }
}

try
{
	$scriptDir = Resolve-Path (Split-Path $MyInvocation.MyCommand.Path)
	$utilsModule = Import-Module -Name "$scriptDir\..\resources\Winpkg.Utils.psm1" -ArgumentList ("HBASE") -PassThru
	Main $scriptDir
}
finally
{
	if( $utilsModule -ne $null )
	{
		Remove-Module $utilsModule
	}
}
