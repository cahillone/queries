--Chad Cahill
--SQL Queries
--2015 March 24

--Provides transaction log space usage statistics for all databases.
--It can be ordered by log space used or log size.
--It can also be used to find statistics on only a particular database. 
CREATE TABLE #TempTable
(
DatabaseName varchar(255),
[Log Size (MB)] float,
[Log Space Used (%)] float,
[Status] int)
insert into #TempTable
execute('dbcc sqlperf(''LogSpace'')')
select log_reuse_wait_desc, name into #TempTable2 from sys.databases

select * from #TempTable INNER JOIN #TempTable2
ON #TempTable.DatabaseName=#TempTable2.name

--where DatabaseName = '' -- use for a particular database
--order by [Log Size (MB)] desc --find the biggest log file
order by [Log Space Used (%)] desc --find the fullest log file
DROP TABLE #TempTable;
DROP TABLE #TempTable2;
--end query----------------------------------------------------------------------------------------

--Daniel's Awesome Query (performance tuning) -------------------
SELECT
es.session_id, es.host_name, es.login_name
, er.status, DB_NAME(er.database_id) AS DatabaseName
, SUBSTRING (qt.text,(er.statement_start_offset/2) + 1,
((CASE WHEN er.statement_end_offset = -1
THEN LEN(CONVERT(NVARCHAR(MAX), qt.text)) * 2
ELSE er.statement_end_offset
END - er.statement_start_offset)/2) + 1) AS [Individual Query]
, qt.text AS [Parent Query]
, es.program_name, er.start_time, qp.query_plan
, er.wait_type, er.total_elapsed_time, er.cpu_time, er.logical_reads
, er.blocking_session_id, er.open_transaction_count, er.last_wait_type
, er.percent_complete
FROM sys.dm_exec_requests AS er
INNER JOIN sys.dm_exec_sessions AS es ON es.session_id = er.session_id
CROSS APPLY sys.dm_exec_sql_text( er.sql_handle) AS qt
CROSS APPLY sys.dm_exec_query_plan(er.plan_handle) qp
WHERE es.is_user_process=1
AND es.session_Id NOT IN (@@SPID)
ORDER BY es.session_id
--------------------------------------

--connections----------------------
DECLARE @AllConnections TABLE(
    SPID INT,
    Status VARCHAR(MAX),
    LOGIN VARCHAR(MAX),
    HostName VARCHAR(MAX),
    BlkBy VARCHAR(MAX),
    DBName VARCHAR(MAX),
    Command VARCHAR(MAX),
    CPUTime INT,
    DiskIO INT,
    LastBatch VARCHAR(MAX),
    ProgramName VARCHAR(MAX),
    SPID_1 INT,
    REQUESTID INT
)

INSERT INTO @AllConnections EXEC sp_who2

select Status, LOGIN, HostName, DBName, Command, ProgramName from @AllConnections where LOGIN = 'tsadmin'
--------------------------------------------------------

--Find all open transactions
	select spid, open_tran, db_name(s.dbid),status,text,loginame,program_name,hostname, * from sys.sysprocesses s
	cross apply sys.dm_exec_sql_text(s.sql_handle) where ((spid >50
	and status <> 'sleeping')
	or spid in (select distinct blocked from sys.sysprocesses where blocked > 0)
	or (status = 'sleeping' and open_tran > 0))
--end query---------------------------------------------------------------------------------------------------------

select * from
sys.sysprocesses
where spid = 82
order by cpu desc

--find long running transactions--------------------------------------------------------------------------
select spid, login_time, last_batch, cmd, cpu, physical_io
from sys.sysprocesses
where (status <> 'sleeping' OR (status = 'sleeping' AND open_tran > 0)) AND spid > 50
order by cpu desc
-------------------------------------------------------------------------------------------------------------

--check for blocking (query from Daniel) ---------------------------------------------------------------------------
select 
a.session_id,
start_time,
b.host_name,
b.program_name,
a.status,
blocking_session_id,
wait_type,
wait_time,
wait_resource,
a.cpu_time,
a.Total_elapsed_time,
scheduler_id,
a.reads,
a.writes,
(SELECT TOP 1 SUBSTRING(s2.text,statement_start_offset / 2+1 , 
     ( (CASE WHEN statement_end_offset = -1 
        THEN (LEN(CONVERT(nvarchar(max),s2.text)) * 2) 
        ELSE statement_end_offset END) - statement_start_offset) / 2+1)) AS sql_statement
from 
sys.dm_exec_requests a inner join sys.dm_exec_sessions b on a.session_id = b.session_id
CROSS APPLY sys.dm_exec_sql_text(a.sql_handle) AS s2
--end query------------------------------------------------------------------------------------------------------

SELECT * FROM sys.dm_exec_query_resource_semaphores

--See all blocking processes
	SELECT
	    spid
	    ,sp.status
	    ,loginame   = SUBSTRING(loginame, 1, 12)
	    ,hostname   = SUBSTRING(hostname, 1, 12)
	    ,blk        = CONVERT(char(3), blocked)
	    ,open_tran
	    ,dbname     = DB_NAME(sp.dbid)
	    ,cmd
	    ,waittype
	    ,waittime
	    ,last_batch, *
	    ,SQLStatement       =
	        SUBSTRING
	        (
	            qt.text,
	            er.statement_start_offset/2,
	            (CASE WHEN er.statement_end_offset = -1
	                THEN LEN(CONVERT(nvarchar(MAX), qt.text)) * 2
	                ELSE er.statement_end_offset
	                END - er.statement_start_offset)/2
	        )
	FROM master.dbo.sysprocesses sp
	LEFT JOIN sys.dm_exec_requests er
	    ON er.session_id = sp.spid
	OUTER APPLY sys.dm_exec_sql_text(er.sql_handle) as qt
	WHERE spid IN (SELECT blocked FROM master.dbo.sysprocesses)
	AND blocked = 0
--end query-------------------------------------------------------------------------


--See what is using up the tempdb space (when tempdb is full)
	SELECT session_id, 
	SUM(internal_objects_alloc_page_count)*8/1024 AS 'Allocted MB',
	SUM(internal_objects_dealloc_page_count)*8/1024 AS 'Dealloc MB' 
	FROM sys.dm_db_task_space_usage    
	GROUP BY session_id order by 'Allocted MB' desc
--end query---------------------------------------------------------------------------


--How long will it take for query to complete?
	SELECT sqltext.TEXT,
	db_name(req.database_id) as dbname,
	req.percent_complete,
	req.estimated_completion_time/1000/60 as ETA_done_min,
	req.total_elapsed_time/1000/60 as total_elapsed_time_min,
	req.start_time,
	req.session_id,
	req.status,
	req.command,
	req.cpu_time,
	req.wait_type,
	req.last_wait_type
	FROM sys.dm_exec_requests req
	CROSS APPLY sys.dm_exec_sql_text(sql_handle) AS sqltext
	where TEXT not like '%SELECT sqltext.TEXT%';
--end query-------------------------------------------------------------------------------------



--Find mount point sizes and freespace
	sp_configure 'show advanced options', 1
	GO
	
	Reconfigure with override
	GO
	
	sp_configure 'xp_cmdshell', 1
	GO
	
	Reconfigure with override
	GO
	
	declare @svrName varchar(255) 
	declare @sql varchar(400) 
	set @svrName = @@SERVERNAME 
	set @sql = 'powershell.exe -c "Get-WmiObject -Class Win32_Volume -Filter ''DriveType = 3'' | select name,capacity,freespace | foreach{$_.name+''|''+$_.capacity/1048576+''%''+$_.freespace/1048576+''*''}"' 
	
	Print @sql
	CREATE TABLE #output 
	(line varchar(255)) 
	insert #output 
	EXEC xp_cmdshell @sql 
	
	select @@SERVERNAME as servername ,rtrim(ltrim(SUBSTRING(line,1,CHARINDEX('|',line) -1))) as drivename 
	,round(cast(rtrim(ltrim(SUBSTRING(line,CHARINDEX('|',line)+1, 
	(CHARINDEX('%',line) -1)-CHARINDEX('|',line)) )) as Float)/1024,0) as 'capacityGB' 
	,round(cast(rtrim(ltrim(SUBSTRING(line,CHARINDEX('%',line)+1, 
	(CHARINDEX('*',line) -1)-CHARINDEX('%',line)) )) as Float) /1024 ,0)as 'freespaceGB', 
	round(100 * (round(cast(rtrim(ltrim(SUBSTRING(line,CHARINDEX('%',line)+1, 
	(CHARINDEX('*',line) -1)-CHARINDEX('%',line)) )) as Float) /1024 ,0))/ 
	(round(cast(rtrim(ltrim(SUBSTRING(line,CHARINDEX('|',line)+1, 
	(CHARINDEX('%',line) -1)-CHARINDEX('|',line)) )) as Float)/1024,0)),0) as percentfree 
	
	from #output 
	where line like '[A-Z][:]%' 
	order by drivename 
	drop table #output
	GO
	
	sp_configure 'xp_cmdshell', 0
	GO
	
	Reconfigure with override
	GO
	
	sp_configure 'show advanced options', 0
	GO
	
	Reconfigure with override
	GO
--end query-------------------------------------------------------------------------


--All DBs and DB files, every file (not rolled up)
	CREATE TABLE #tmpData
	(    
	    dbname varchar(50),
	    filegroup varchar(50),
	    filesize float,
	    maxsize float,
	    growth int, 
	    space_used_MB float,
	    free_space_MB float,
	    free_pct float,
	    lname varchar(256),
	    fname varchar(256)
	)
	
	insert into #tmpData
	execute sp_msforeachdb @command1 = 'use [?]; 
	
	select
		db_name(db_id()) as ''database'', fg.name as filegroup,
		convert(decimal(12,2),round(a.size/128.000,2)) as filesize,	
		convert(decimal(12,2),round(a.maxsize/128.000,2)) as maxsize,
		a.growth,
		convert(decimal(12,2),round(fileproperty(a.name,''SpaceUsed'')/128.000,2)) as space_used_MB,	
		convert(decimal(12,2),round((a.size-fileproperty(a.name,''SpaceUsed''))/128.000,2)) as free_space_MB,	
		convert(decimal(12,3),convert(decimal(12,2),(a.size-fileproperty(a.name,''SpaceUsed'')))/convert(decimal(12,2),a.size))*100 as [free %],
		NAME = a.NAME,
		FILENAME = a.FILENAME
	from
		sys.sysfiles a left join sys.filegroups fg on a.groupid = fg.data_space_id
		join sys.databases sd on db_name(db_id()) = sd.name
		order by fg.name ASC'
		
		select *
		from #tmpData
		where dbname not in ('master','model','tempdb','msdb')
		--and lname like '%-%'
		--and filegroup != 'PRIMARY'
		--and fname like 'f:\t%'
		--and fname like '
		--order by convert(decimal(12,2),round(filesize-(space_used_MB/.75),2)) desc
		--order by space_used_MB
		--order by filesize asc
		order by dbname asc
		drop table #tmpData
--end query--------------------------------------------------------------------------------------

--Filegroups rolled up with amount needed for 25% free
	CREATE TABLE #tmpData
	(    
	    filegroup varchar(50),
	    dbname varchar(50),
	    maxsize float,
	    filesize float,
	    space_used_MB float,
	    free_space_MB float,
	    free_pct float
	)
	
	insert into #tmpData
	execute sp_msforeachdb @command1 = 'use [?]; 
	
	select
		fg.name as filegroup, db_name(db_id()) as ''database'',
		sum(convert(decimal(12,2),round(a.maxsize/128.000,2))) as maxsize,
		sum(convert(decimal(12,2),round(a.size/128.000,2))) as filesize,
		sum(convert(decimal(12,2),round(fileproperty(a.name,''SpaceUsed'')/128.000,2))) as space_used_MB,
		sum(convert(decimal(12,2),round((a.size-fileproperty(a.name,''SpaceUsed''))/128.000,2))) as free_space_MB,	
		(sum(convert(decimal(12,3),convert(decimal(12,2),(a.size-fileproperty(a.name,''SpaceUsed''))))) / sum(convert(decimal(12,2),a.size)))*100 as [free %]
	from
		sys.sysfiles a left join sys.filegroups fg on a.groupid = fg.data_space_id
		group by fg.name'
		
		select *
		,convert(decimal(12,2),round(space_used_MB/.75,2)) as 'New filesize for 25% free'
		,convert(decimal(12,2),round(filesize-(space_used_MB/.75),2)) as 'Releasable'
		from #tmpData
		where dbname not in ('master','model','tempdb','msdb') and filegroup is not null
		order by convert(decimal(12,2),round(filesize-(space_used_MB/.75),2)) desc
		drop table #tmpData
--end query---------------------------------------------------------------------------------

------test alert------------------------------------------------------------------------------
RAISERROR('This is a test alert chad.cahill@hpe.com (close alert after 5 mins)',10,1) with log
-----------------------------------------------------------------------------------------------

----change owner of every user database to SA-------
sp_msforeachdb 'use [?]; EXEC dbo.sp_changedbowner @loginame = sa, @map = false'
----------------------------------------------------

select name from sys.databases


ALTER DATABASE MTMaster_11 MODIFY FILE ( NAME = N'MTMaster_11_log', MAXSIZE = 50000MB, FILEGROWTH = 100MB)

ALTER DATABASE MTMaster_12 MODIFY FILE ( NAME = N'MTMaster_12', SIZE = 100MB )

ALTER DATABASE MTMaster_15 MODIFY FILE ( NAME = N'MTMaster_15', SIZE = 100MB )

ALTER DATABASE MTMaster_17 MODIFY FILE ( NAME = N'MTMaster_17', SIZE = 100MB )




--add file, modify file, increase file size

ALTER DATABASE [DE_Catalog] MODIFY FILE ( NAME = N'DE_Catalog', SIZE = 20480KB , MAXSIZE = 204800KB )

ALTER DATABASE [DE_Catalog] MODIFY FILE ( NAME = N'DE_Catalog_log', SIZE = 20480KB , MAXSIZE = 204800KB )

ALTER DATABASE [DE_Catalog] ADD FILE ( NAME = N'DE_Catalog_1', FILENAME = N'F:\MSSQL\MSSQL.1\MSSQL\Data\DE_Catalog_1.ndf' , SIZE = 5120KB , MAXSIZE = 102400KB , FILEGROWTH = 10240KB ) TO FILEGROUP [PRIMARY]

ALTER DATABASE [DE_Catalog] ADD LOG FILE ( NAME = N'DE_Catalog_log_1', FILENAME = N'K:\MSSQL\MSSQL.1\MSSQL\Data\DE_Catalog_log_1.ldf' , SIZE = 102400KB , MAXSIZE = 102400KB , FILEGROWTH = 10240KB )

--end modify/add file queries-----------------------------------------------------------------------


--User account login info - show expired password date or locked account
	SELECT name,
	LOGINPROPERTY([name], 'PasswordLastSetTime') AS 'PasswordChanged',
	LOGINPROPERTY([name], 'IsExpired') AS 'Is Expired',
	LOGINPROPERTY([name], 'IsLocked') AS 'Is Locked',
	LOGINPROPERTY([name], 'LockoutTime') AS 'Lockout Time',
	LOGINPROPERTY([name], 'DaysUntilExpiration') AS 'DaysUntilExpiration',
	LOGINPROPERTY([name], 'PasswordLastSetTime') AS 'PasswordLastSetTime',
	LOGINPROPERTY([name], 'BadPasswordCount') AS 'BadPasswordCount',
	LOGINPROPERTY([name], 'BadPasswordTime') AS 'BadPasswordTime'
	
	FROM sys.sql_logins where name = 'acctname'
--end query-----------------------------------------------------------------------


--To unlock accounts
-------------------------------------------------------
alter login [loginname] with check_policy = off
-- unlock account with GUI
alter login [loginname] with check_policy = on
------------------------------------------------------


--read error log descending or ascending
--can use to check how long SQL Server has been up
xp_readerrorlog 0,1,null,NULL,NULL,NULL,N'desc'
xp_readerrorlog 0,1,null,NULL,NULL,NULL,N'asc'
--end query-------------------------------------------------------------------------

-- hp execute role
USE [DBName]
GO
CREATE ROLE [hp_sp_role] AUTHORIZATION [dbo]
GO
GRANT EXECUTE TO [hp_sp_role]
GO
--------------

--logins/users...-------------------------------------------
SELECT LOGINPROPERTY('RSExec','PASSWORDHASH');

CREATE LOGIN RSExec WITH PASSWORD = <HEX Value> HASHED; 

USE [master]
GO
CREATE LOGIN [loginName] WITH PASSWORD=N'mypassword', DEFAULT_DATABASE=[master], CHECK_EXPIRATION=ON, CHECK_POLICY=ON
GO

use [DBName]
go
EXEC sp_change_users_login 'Auto_Fix', 'accountName'
go

--show permissions
use [DBName]
select  db_name(), princ.name
,       princ.type_desc
,       perm.permission_name
,       perm.state_desc
,       perm.class_desc
,       object_name(perm.major_id)
from    sys.database_principals princ
left join
        sys.database_permissions perm
on      perm.grantee_principal_id = princ.principal_id
where permission_name = 'create table'

-----------------------------------------------------------------------------------

--check last backup date-------------------------------------
SELECT sdb.Name AS db_name,
COALESCE(CONVERT(VARCHAR(12), MAX(bus.backup_finish_date), 101),'-') AS last_backup,
CASE
       WHEN bus.type = 'D' THEN 'Full'
       WHEN bus.type = 'I' THEN 'Diff'
       END as type
FROM sys.sysdatabases sdb
LEFT OUTER JOIN msdb.dbo.backupset bus ON bus.database_name = sdb.name
WHERE bus.type <> 'L'
and bus.name LIKE 'Data Protector%'
GROUP BY sdb.Name, bus.type
ORDER BY db_name; 
------------------------------------------------------------------------------------


--order wait types by percentage
---------------------------------------------------
WITH Waits AS
    (SELECT
        wait_type,
        wait_time_ms / 1000.0 AS WaitS,
        (wait_time_ms - signal_wait_time_ms) / 1000.0 AS ResourceS,
        signal_wait_time_ms / 1000.0 AS SignalS,
        waiting_tasks_count AS WaitCount,
        100.0 * wait_time_ms / SUM (wait_time_ms) OVER() AS Percentage,
        ROW_NUMBER() OVER(ORDER BY wait_time_ms DESC) AS RowNum
    FROM sys.dm_os_wait_stats
    WHERE wait_type NOT IN (
        'CLR_SEMAPHORE', 'LAZYWRITER_SLEEP', 'RESOURCE_QUEUE', 'SLEEP_TASK',
        'SLEEP_SYSTEMTASK', 'SQLTRACE_BUFFER_FLUSH', 'WAITFOR', 'LOGMGR_QUEUE',
        'CHECKPOINT_QUEUE', 'REQUEST_FOR_DEADLOCK_SEARCH', 'XE_TIMER_EVENT', 'BROKER_TO_FLUSH',
        'BROKER_TASK_STOP', 'CLR_MANUAL_EVENT', 'CLR_AUTO_EVENT', 'DISPATCHER_QUEUE_SEMAPHORE',
        'FT_IFTS_SCHEDULER_IDLE_WAIT', 'XE_DISPATCHER_WAIT', 'XE_DISPATCHER_JOIN', 'BROKER_EVENTHANDLER',
        'TRACEWRITE', 'FT_IFTSHC_MUTEX', 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
        'BROKER_RECEIVE_WAITFOR', 'ONDEMAND_TASK_QUEUE', 'DBMIRROR_EVENTS_QUEUE',
        'DBMIRRORING_CMD', 'BROKER_TRANSMITTER', 'SQLTRACE_WAIT_ENTRIES',
        'SLEEP_BPOOL_FLUSH', 'SQLTRACE_LOCK')
    )
SELECT
    W1.wait_type AS WaitType, 
    CAST (W1.WaitS AS DECIMAL(14, 2)) AS Wait_S,
    CAST (W1.ResourceS AS DECIMAL(14, 2)) AS Resource_S,
    CAST (W1.SignalS AS DECIMAL(14, 2)) AS Signal_S,
    W1.WaitCount AS WaitCount,
    CAST (W1.Percentage AS DECIMAL(4, 2)) AS Percentage,
    CAST ((W1.WaitS / W1.WaitCount) AS DECIMAL (14, 4)) AS AvgWait_S,
    CAST ((W1.ResourceS / W1.WaitCount) AS DECIMAL (14, 4)) AS AvgRes_S,
    CAST ((W1.SignalS / W1.WaitCount) AS DECIMAL (14, 4)) AS AvgSig_S
FROM Waits AS W1
    INNER JOIN Waits AS W2 ON W2.RowNum <= W1.RowNum
GROUP BY W1.RowNum, W1.wait_type, W1.WaitS, W1.ResourceS, W1.SignalS, W1.WaitCount, W1.Percentage
HAVING SUM (W2.Percentage) - W1.Percentage < 95; -- percentage threshold
GO 
----------------------------------------------------

--Find Database Fragmentation
USE [DBName]
select dbschemas.[name] as 'Schema',
	dbtables.[name] as 'Table',
	dbindexes.[name] as 'Index',
	indexstats.avg_fragmentation_in_percent,
	indexstats.page_count
from sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL, NULL, NULL) as indexstats
	INNER JOIN sys.tables dbtables on dbtables.[object_id] = indexstats.[object_id]
	INNER JOIN sys.schemas dbschemas on dbtables.[schema_id] = dbschemas.[schema_id]
	INNER JOIN sys.indexes as dbindexes on dbindexes.[object_id] = indexstats.[object_id]
		AND indexstats.index_id = dbindexes.index_id
where indexstats.database_id = DB_ID()
order by indexstats.avg_fragmentation_in_percent desc; 
-----------------------------------------------------

-- Clear wait stats (performance tuning)
DBCC SQLPERF("sys.dm_os_wait_stats",CLEAR); 
-----------------------------------------

-- Check wait stats (performance tuning)
select * from sys.dm_os_wait_stats
order by wait_time_ms desc
---------------------------------------------

-- Check instance settings
sp_configure 'show_advanced_options', 1
reconfigure
sp_configure
---------------------------------------------
