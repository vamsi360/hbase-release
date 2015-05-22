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
### Install script that can be used to install HBase as a Single-Node cluster.
### To invoke the scipt, run the following command from PowerShell:
###   install.ps1 -username <username> -password <password> or
###   install.ps1 -credentialFilePath <credentialFilePath>
###
### where:
###   <username> and <password> represent account credentials used to run
###   HBase services as Windows services.
###   <credentialFilePath> encripted credentials file path
###
### By default, Hadoop is installed to "C:\Hadoop". To change this set
### HADOOP_NODE_INSTALL_ROOT environment variable to a location were
### you'd like Hadoop installed.
###
### Script pre-requisites:
###   JAVA_HOME must be set to point to a valid Java location.
###   HADOOP_HOME must be set to point to a valid Hadoop install location.
###
### To uninstall previously installed Single-Node cluster run:
###   uninstall.ps1
###
### NOTE: Notice @version@ strings throughout the file. First compile
### winpkg with "ant winpkg", that will replace the version string.
###

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
    [String]    
    $hbaseRole
    )

function Main( $scriptDir )
{
    $FinalName = "hbase-@version@"
    if ( -not (Test-Path ENV:WINPKG_LOG))
    {
        $ENV:WINPKG_LOG = "$FinalName.winpkg.log"
    }
    
    $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"
    $nodeInstallRoot = "$ENV:HADOOP_NODE_INSTALL_ROOT"

    ###
    ### Create the Credential object from the given username and password or the provided credentials file
    ###
    $serviceCredential = Get-HadoopUserCredentials -credentialsHash @{"username" = $username; "password" = $password; `
        "passwordBase64" = $passwordBase64; "credentialFilePath" = $credentialFilePath}
    $username = $serviceCredential.UserName
    Write-Log "Username: $username"
    Write-Log "CredentialFilePath: $credentialFilePath"

    ###
    ### Begin install
    ###
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
        $hbaseRole = $hbaseRole+" "+"hbrest"
        $hbaseRole = $hbaseRole+" "+"thrift"
        $hbaseRole = $hbaseRole+" "+"thrift2"
    }

    $hbaseRole = $hbaseRole.Trim()
    Write-Log "Roles are $hbaseRole"
    Install "hbase" $nodeInstallRoot $serviceCredential $hbaseRole


    ###
    ### Apply hbase-site.xml configuration changes
    ###

    Write-Log "Updating hbase site configuration"

    $hbaseRootDir = "hdfs://${ENV:NAMENODE_HOST}:8020/apps/hbase/data"

    if ($ENV:HA -ieq "yes") {
        $hbaseRootDir = "hdfs://${ENV:HA_CLUSTER_NAME}/apps/hbase/data"
    }
        
    $config += @{
        "hbase.rootdir" = "$hbaseRootDir";
        "hbase.zookeeper.quorum" = "$ENV:ZOOKEEPER_HOSTS";
        "hbase.zookeeper.property.clientPort" = "2181"}
    if ($ENV:IS_PHOENIX -ieq "yes") 
    {
        $config += @{"hbase.regionserver.wal.codec" = "org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec";
                     "hbase.region.server.rpc.scheduler.factory.class" = "org.apache.hadoop.hbase.ipc.PhoenixRpcSchedulerFactory";
                     "hbase.rpc.controllerfactory.class" = "org.apache.hadoop.hbase.ipc.controller.ServerRpcControllerFactory"	
                    }
    }
    configure "hbase" $nodeInstallRoot $serviceCredential $config


    Write-Log "Installation of hbase Core complete"
}

try
{
    $scriptDir = Resolve-Path (Split-Path $MyInvocation.MyCommand.Path)
    $utilsModule = Import-Module -Name "$scriptDir\..\resources\Winpkg.Utils.psm1" -ArgumentList ("HBASE") -PassThru
    $apiModule = Import-Module -Name "$scriptDir\InstallApi.psm1" -PassThru
    Main $scriptDir
}
catch [Exception]
{
    Write-Log $_.Exception.Message $_
}
finally
{
    if( $apiModule -ne $null )
    {
        Remove-Module $apiModule
    }

    if( $utilsModule -ne $null )
    {
        Remove-Module $utilsModule
    }
}
