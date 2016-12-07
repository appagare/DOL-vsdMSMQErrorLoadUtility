use dolLogEvent
GO

declare @AppID as int
declare @AppName as varchar(50)
declare @DeveloperEmail as varchar(50)
set @AppName = 'vsdMSMQErrorLoadUtility'
set @DeveloperEmail = 'MLUND@DOL.WA.GOV'

-- everything below this line can remain "as is"

if (select count(*) from buApplication where AppName=@AppName)=0
	begin
		insert into buApplication
		select @AppName
	
		set @AppID = @@IDENTITY
	end
else
	begin
		set @AppID = (select top 1 AppID from buApplication where AppName=@AppName)
	end

if UPPER(@@SERVERNAME) = 'DOLDBOLYDEV02'
	begin
		insert into buApplicationEmail
		select @AppID, @DeveloperEmail
	end
else
	begin
		insert into buApplicationEmail
		select @AppID, 'VSAPPSUP@DOL.WA.GOV'
	end
