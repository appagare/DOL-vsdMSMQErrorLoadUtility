Option Strict On
Option Explicit On 

Module Process

    'constants
    Private Const APPLICATION_ERROR As Integer = -1
    Private Const APPLICATION_OK As Integer = 0
    Private Const SP_DELETE As String = "delException"
    Private Const SP_FETCH As String = "selExceptions"
    Private Const ERR01 As String = "01"
    Private Const ERR02 As String = "02"
    Private Const ERR03 As String = "03"
    Private Const DP_PROCESS As String = "vsdDPImmediateUpdate"
    Private Const VFS_PROCESS As String = "vsdVFSImmediateUpdate"
    
    'module level parameter
    Private strAppName As String = Replace(Right(System.Reflection.Assembly.GetExecutingAssembly.Location, _
            Len(System.Reflection.Assembly.GetExecutingAssembly.Location) - _
            InStrRev(System.Reflection.Assembly.GetExecutingAssembly.Location, "\")), ".exe", "", , , CompareMethod.Text)


    Sub Main()

        On Error GoTo Err_Handler

        'log the process start
        LogEvent("Main", "Start", WA.DOL.LogEvent.LogEvent.MessageType.Start)

        'get the required parameters
        Dim AppSettingsReader As New System.Configuration.AppSettingsReader
        Dim strConnectStringKey As String = CType(AppSettingsReader.GetValue("DatabaseKey", GetType(System.String)), String)
        'special test mode that doesn't delete records after recovery
        Dim intTestMode As Integer = 0
        'special case setting that handles the case if duplicates exist AND 
        'the subsequent record got farther in the process than the orignal
        Dim intSortOrder As Integer = 0

        Dim SQL As WA.DOL.Data.SqlHelper
        Dim intCount As Integer = 0 'informational counter
        Dim strProcess As String = "" 'process(es) to handle
        Dim strErr01 As String = "" 'determines whether 01 records are processed
        Dim strErr02 As String = "" 'determines whether 02 records are processed
        Dim strErr03 As String = "" 'determines whether 03 records are processed
        Dim dsSettings As New DataSet
        Dim r As DataRow
        Dim Cache As New Hashtable ' memory cache for detecting VFS duplicates
        Dim DPCache As New Hashtable ' memory cache for detecting DP duplicates
        Dim dtTranTypes As New DataTable 'data table of all of the VFS tran types we will be handling
        Dim dtDPTranTypes As New DataTable 'data table of all of the DP tran types we will be handling
        Dim dtQueueList As New DataTable 'data table of all of the queue's we will be handling
        Dim intExceptionID As Integer = 0 'capture the current exception id record - useful for debugging

        'detect test/normal delete mode
        If LCase(CType(AppSettingsReader.GetValue("TestMode", GetType(System.String)), String)) = "true" Then
            intTestMode = 1
        End If

        'detect special case/normal sort order mode
        If LCase(CType(AppSettingsReader.GetValue("SortOrder", GetType(System.String)), String)) = "1" Then
            intSortOrder = 1
        End If

        'make sure we have required values
        If Trim(strConnectStringKey) = "" Then
            'log error 
            LogEvent("Main", "Missing required parameters.", WA.DOL.LogEvent.LogEvent.MessageType.Error)
            'call common shutdown routine
            ApplicationExit(APPLICATION_ERROR)
        End If

        'get the run-time parameters
        dsSettings = SQL.ExecuteDataset(strConnectStringKey, CommandType.StoredProcedure, _
                "selAppConfig", New SqlClient.SqlParameter("@strProcess", strAppName))

        For Each r In dsSettings.Tables(0).Rows
            Select Case LCase(CType(r("Name"), String))
                Case "err01"
                    If CType(r("Value"), Integer) = 1 Then
                        ' ignore all other values
                        strErr01 = ERR01
                    End If
                Case "err02"
                    If CType(r("Value"), Integer) = 1 Then
                        ' ignore all other values
                        strErr02 = ERR02
                    End If
                Case "err03"
                    If CType(r("Value"), Integer) = 1 Then
                        ' ignore all other values
                        strErr03 = ERR03
                    End If
                Case "process"
                    strProcess = Trim(CType(r("Value"), String))
            End Select
        Next
        'get the VFS tran types table
        LogEvent("Main", "Fetching VFS TranTypes...", WA.DOL.LogEvent.LogEvent.MessageType.Information)
        dtTranTypes = SQL.ExecuteDataset(strConnectStringKey, CommandType.StoredProcedure, _
            "selTranTypes", New SqlClient.SqlParameter("@strProcess", strProcess)).Tables(0)

        'get the DP tran types table
        LogEvent("Main", "Fetching DP TranTypes...", WA.DOL.LogEvent.LogEvent.MessageType.Information)
        dtDPTranTypes = SQL.ExecuteDataset(strConnectStringKey, CommandType.StoredProcedure, _
            "selDPTranTypes", New SqlClient.SqlParameter("@strProcess", strProcess)).Tables(0)

        'get the common queue list table
        LogEvent("Main", "Fetching QueueList...", WA.DOL.LogEvent.LogEvent.MessageType.Information)
        dtQueueList = SQL.ExecuteDataset(strConnectStringKey, CommandType.StoredProcedure, _
            "selQueueList", New SqlClient.SqlParameter("@strProcess", strProcess)).Tables(0)

        'fetch matching data
        LogEvent("Main", "Fetching data...", WA.DOL.LogEvent.LogEvent.MessageType.Information)
        Dim DataReader As SqlClient.SqlDataReader = SQL.ExecuteReader(strConnectStringKey, CommandType.StoredProcedure, SP_FETCH, _
            New SqlClient.SqlParameter("@strProcess", strProcess), _
            New SqlClient.SqlParameter("@01", strErr01), _
            New SqlClient.SqlParameter("@02", strErr02), _
            New SqlClient.SqlParameter("@03", strErr03), _
            New SqlClient.SqlParameter("@intSortOrder", intSortOrder))


        'loop through data appending it to the file
        LogEvent("Main", "Starting recovery...", WA.DOL.LogEvent.LogEvent.MessageType.Information)
        While DataReader.Read

            intExceptionID = CType(DataReader("ExceptionID"), Integer)

            'parseXML and get buffer
            Dim strBuffer As String = GetBuffer(CType(DataReader("Message"), String))  ' get the buffer portion of the MSMQ message from the XML
            Dim strMessageProcess As String = CType(DataReader("Process"), String)

            
            Select Case UCase(strMessageProcess)
                Case UCase(DP_PROCESS)

                    'get tran type
                    Dim strTranType As String = Trim(Left(strBuffer, 2)) 'get the tran type from the buffer

                    'instantiate the TranTypeUtil
                    Dim DPTranTypeUtil As New DPTranTypeUtility(strTranType, dtDPTranTypes)  'get utility object for this TranType

                    Dim intCurrentRecordStatus As Integer = -1

                    'duplicate in cache? - ignore errors 
                    If DPCache.Contains(Trim(strBuffer)) = False Then
                        'record not in cache. See if it still exists in the db

                        'get request/status - this process only cares if the final call was completed. If not, 
                        'the vsdDPImmediateUpdate service should handle detecting at which
                        'point the calls need to resume
                        intCurrentRecordStatus = DPTranTypeUtil.GetRequest(strBuffer, DPTranTypeUtil.CallCount - 1)

                        'record found?
                        If intCurrentRecordStatus >= 0 AndAlso intCurrentRecordStatus < DPTranTypeUtil.CallCount Then
                            'a record was found and its update status is less than the call count so it is not completed yet

                            'write to queue
                            QueueWrite(GetQueuePathFromProcess(dtQueueList, CType(DataReader("Process"), String)), CType(DataReader("Message"), String))

                            'add buffer to cache
                            DPCache.Add(Trim(strBuffer), Trim(strBuffer))
                        End If 'record not found or record completed
                    End If 'record is in cache so has already been added to the queue

                Case UCase(VFS_PROCESS)

                    'get tran type
                    Dim strTranType As String = Trim(Left(strBuffer, 4)) 'get the tran type from the buffer

                    'instantiate the TranTypeUtil
                    Dim TranTypeUtil As New TranTypeUtility(strMessageProcess, strTranType, dtTranTypes) 'get utility object for this TranType

                    Dim intCurrentRecordStatus As Integer = -1

                    'duplicate in cache? - ignore errors 
                    If Cache.Contains(Trim(strBuffer)) = False Then
                        'record not in cache. See if it still exists in the db

                        'get request/status - this process only cares if the final call was completed. If not, 
                        'the vsdVFSImmediateUpdate or vsdDPImmediateUpdate services should handle detecting at which
                        'point the calls need to resume
                        intCurrentRecordStatus = TranTypeUtil.GetRequest(strBuffer, TranTypeUtil.CallCount - 1)

                        'record found?
                        If intCurrentRecordStatus >= 0 AndAlso intCurrentRecordStatus < TranTypeUtil.CallCount Then
                            'a record was found and its update status is less than the call count so it is not completed yet

                            'write to queue
                            QueueWrite(GetQueuePathFromProcess(dtQueueList, CType(DataReader("Process"), String)), CType(DataReader("Message"), String))

                            'add buffer to cache
                            Cache.Add(Trim(strBuffer), Trim(strBuffer))
                        End If 'record not found or record completed
                    End If 'record is in cache so has already been added to the queue
                Case Else
                    'shouldn't happen but log/abort

            End Select

            'delete record if in normal mode
            If intTestMode <> 1 Then
                'if not in test mode, delete the record from the exception table
                Call SQL.ExecuteNonQuery(strConnectStringKey, CommandType.StoredProcedure, SP_DELETE, _
                    New SqlClient.SqlParameter("@intExceptionID", CType(DataReader("ExceptionID"), Integer)))
            End If

            'increment a counter for informational purposes
            intCount += 1
        End While

        'log the counter
        LogEvent("Main", CStr(intCount) & " records processed.", WA.DOL.LogEvent.LogEvent.MessageType.Information)

        If intTestMode = 1 Then
            'log a message that we are in test mode in case user wonders why messages didn't get deleted from exception table
            LogEvent("Main", "Process is in TEST MODE. No records were deleted!", WA.DOL.LogEvent.LogEvent.MessageType.Information)
        End If

        'call common shutdown routine with OK
        ApplicationExit(APPLICATION_OK)

Err_Handler:
        Dim intErrorCode As Integer = Math.Abs(Err.Number) * -1
        Dim RecordMessage As String = ""
        If intExceptionID > 0 Then
            'if a particular record caused the error, let's note it.
            RecordMessage = " (ExceptionID=" & intExceptionID.ToString & ")"
        End If

        'log error
        LogEvent("Err_Handler", Err.Description & RecordMessage, WA.DOL.LogEvent.LogEvent.MessageType.Error)

        'make sure we always throw a non-zero error here
        If intErrorCode = 0 Then
            intErrorCode = APPLICATION_ERROR
        End If

        'call common shutdown routine
        ApplicationExit(intErrorCode)
    End Sub
    ''' <summary>
    '''     Common routine to log "Finished" message and exit with the specified exit code.
    ''' </summary>
    ''' <param name="ExitCode">integer that will returned.</param>
    Private Sub ApplicationExit(ByVal ExitCode As Integer)

        'log finished message
        LogEvent("Main", "Finished", WA.DOL.LogEvent.LogEvent.MessageType.Finish)

        'exit 
        System.Environment.Exit(ExitCode)
    End Sub
    ''' <summary>
    '''     Returns the content of the <buffer></buffer> element from the complete MSMQ message.
    ''' </summary>
    ''' <param name="Message">Entire MSMQ message.</param>
    ''' <remarks>
    '''     The MSMQ message is an XML string. The actual data that the calling program
    '''     generated is contained within the <buffer></buffer> element.
    '''     This function parses the XML to obtain the contents of the <buffer> element 
    '''     (i.e. - returns the original data that the calling program generated).
    '''     This function doesn't have error handling by design. Should an error occur,
    '''     such as the XML fail to load and/or parse because of bad input, the calling 
    '''     routine will handle the error.
    ''' </remarks>
    Private Function GetBuffer(ByVal Message As String) As String

        Dim XML As New Xml.XmlDocument
        XML.LoadXml(Message)
        Return XML.SelectSingleNode("qmsg/buffer").FirstChild.Value

    End Function
    ''' <summary>
    '''     Returns the full queue path to use, as determined by the parent Process of the exception record.
    ''' </summary>
    ''' <param name="QueueTable">Data table of all queues being handled.</param>
    ''' <param name="Process">Name of the parent process whose queue needs to be located in the table.</param>
    ''' <remarks>
    ''' </remarks>
    Private Function GetQueuePathFromProcess(ByVal QueueTable As DataTable, _
        ByVal ProcessName As String) As String

        'locate the queue entry by process name
        QueueTable.DefaultView.RowFilter = "Process='" & ProcessName & "'"
        If QueueTable.DefaultView.Count > 0 Then
            'at least one, so get it

            Dim ServerName As String = Replace(Replace(Trim(CType(QueueTable.DefaultView(0)("Server"), String)), "PRIVATE$", "", , , CompareMethod.Text), "\", "")
            Dim QueueName As String = Replace(Replace(Trim(CType(QueueTable.DefaultView(0)("Queue"), String)), "PRIVATE$", "", , , CompareMethod.Text), "\", "")

            QueueTable.DefaultView.RowFilter = ""
            'make sure we have the minimum requirements to proceed (this should never happen)
            If ServerName = "" OrElse QueueName = "" Then
                Throw New Exception("GetQueuePathFromProcess had Missing parameters.")
            End If

            'construct the full QueuePath
            Return ServerName & "\PRIVATE$\" & QueueName

        Else
            'shouldn't happen unless there is a configuration problem such as
            'the process name logging the exceptions doesn't match the process
            'name defined in the queue list table. Throw and error.
            QueueTable.DefaultView.RowFilter = ""
            Throw New Exception("GetQueuePathFromProcess failed to locate a queue for process '" & _
                ProcessName & "'.")

        End If 'match found

    End Function
    ''' <summary>
    '''     Common event logging routine.
    ''' </summary>
    ''' <param name="Source">source location that is logging the event.</param>
    ''' <param name="Message">message to log.</param>
    ''' <param name="MessageType">type of message to log (Start, Informational, Debug, Error, Finish).</param>
    ''' <remarks>
    ''' </remarks>
    Private Sub LogEvent(ByVal Source As String, _
        ByVal Message As String, _
        ByVal MessageType As WA.DOL.LogEvent.LogEvent.MessageType)
       
        Dim LogEventObject As New WA.DOL.LogEvent.LogEvent

        If MessageType = WA.DOL.LogEvent.LogEvent.MessageType.Error Then
            'do a standard log and e-mail
            LogEventObject.LogEvent(strAppName, Source, Message, MessageType, WA.DOL.LogEvent.LogEvent.LogType.Standard)
            LogEventObject.LogEvent(strAppName, Source, Message, MessageType, WA.DOL.LogEvent.LogEvent.LogType.Email)
        Else
            'standard only
            LogEventObject.LogEvent(strAppName, Source, Message, MessageType, WA.DOL.LogEvent.LogEvent.LogType.Standard)
        End If

        LogEventObject = Nothing

    End Sub

    ''' <summary>
    '''     Sends a message to the queue.
    ''' </summary>
    ''' <param name="QueuePath">The full pathname to the queue.</param>
    ''' <param name="Message">String containing the entire queue message.</param>
    ''' <remarks>
    '''     Only supports private queues.
    '''     The size of the Message parameter cannot exceed 4 MB.
    ''' </remarks>
    Private Sub QueueWrite(ByVal QueuePath As String, _
        ByVal Message As String)

        'The MsmqHelper class cannot be inherited from, thus no New constructor
        Dim MSMQHelper As WA.DOL.MsmqHelper

        'the simplest method is to pass the complete server\queue name and the queue message
        MSMQHelper.SendMessage(QueuePath, Message, True)

    End Sub

End Module
