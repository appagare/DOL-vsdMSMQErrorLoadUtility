Option Strict On
Option Explicit On 

''' <summary>
'''     This is a helper class that handles all of the TranType specific tasks.
''' </summary>
Friend Class DPTranTypeUtility
    Private DataObject As WA.DOL.Data.SqlHelper 'common Data object
    Private _TranType As String = ""
    Private _TranTypes As New DataTable
    Private _Index As Byte = 0
    Private _XML As New Xml.XmlDocument

    ''' <summary>
    '''     Returns the number of web service calls for the current tran code
    ''' </summary>
    Friend ReadOnly Property CallCount() As Byte
        Get
            Dim ReturnValue As Byte = 0
            If _TranType <> "" Then
                ReturnValue = CType(_TranTypes.DefaultView.Count, Byte)
            End If
            Return ReturnValue
        End Get
    End Property
    ''' <summary>
    '''     Sets/returns the index for the web service calls
    ''' </summary>
    Friend Property Index() As Byte
        Get
            Return _Index
        End Get
        Set(ByVal Value As Byte)
            _Index = Value
        End Set
    End Property
    ''' <summary>
    '''     Returns the WebService's response length based on the TranType and Index.
    ''' </summary>
    ''' <param name="Index">Record index between zero and record count - 1.</param>
    Friend ReadOnly Property ResponseLength(ByVal Index As Integer) As Integer
        Get
            If CallCount > 0 Then
                Return CType(_TranTypes.DefaultView(Index)("ResponseLength"), Integer)
            Else
                Return 0
            End If
        End Get
    End Property
    ''' <summary>
    '''     Returns the WebService's response starting position based on the TranType and Index.
    ''' </summary>
    ''' <param name="Index">Record index between zero and record count - 1.</param>
    Friend ReadOnly Property ResponseOffset(ByVal Index As Integer) As Integer
        Get
            If CallCount > 0 Then
                Return CType(_TranTypes.DefaultView(Index)("ResponseOffset"), Integer)
            Else
                Return 0
            End If
        End Get
    End Property
    ''' <summary>
    '''     Creates or updates the message's web method status and returns the updated XML document as a string.
    ''' </summary>
    ''' <param name="XML">XMLDocument object that will be modified.</param>
    Friend Function SetProcessCallStatus(ByRef XML As Xml.XmlDocument, ByVal Status As String) As String
        'creates or updates the message's web method status
        If Me.ProcessNodeExists(XML) = False Then
            Me.CreateProcessNode(XML)
        End If
        XML.SelectSingleNode("qmsg/process/@status").FirstChild.Value = Status
        XML.SelectSingleNode("qmsg/process/@datetime").FirstChild.Value = FormatDateTime(Now)
        'returns the complete XML as a string so it updates the Message in case it has to be written to the exception table or queue
        Return XML.OuterXml
    End Function

    ''' <summary>
    '''     Returns True if the "process" node of the xml document exists. Otherwise, returns false.
    ''' </summary>
    ''' <param name="XML">XMLDocument object that is checked for the "process" node.</param>
    Friend Function ProcessNodeExists(ByRef XML As Xml.XmlDocument) As Boolean
        If Not XML.SelectSingleNode("qmsg/process") Is Nothing Then
            Return True
        Else
            Return False
        End If
    End Function
    ''' <summary>
    '''     Create the "process" node of the xml document if it doesn't exist.
    ''' </summary>
    ''' <param name="XML">XMLDocument object on which the node will be created.</param>
    Private Sub CreateProcessNode(ByRef XML As Xml.XmlDocument)
        If ProcessNodeExists(XML) = True Then
            'already exists, don't create another one
            Exit Sub
        End If

        Dim Element As Xml.XmlElement = XML.CreateElement("process")
        Dim Attr As Xml.XmlNode
        'status indicates the index of successful process calls
        Attr = XML.CreateNode(System.Xml.XmlNodeType.Attribute, "status", Nothing)
        Attr.Value = "0"
        Element.Attributes.Append(CType(Attr, Xml.XmlAttribute))

        'datetime indicates the datetime of the status update
        Attr = XML.CreateNode(System.Xml.XmlNodeType.Attribute, "datetime", Nothing)
        Attr.Value = FormatDateTime(Now)
        Element.Attributes.Append(CType(Attr, Xml.XmlAttribute))

        'attempt indicates the how many times this message has been processed
        Attr = XML.CreateNode(System.Xml.XmlNodeType.Attribute, "attempt", Nothing)
        Attr.Value = "0"
        Element.Attributes.Append(CType(Attr, Xml.XmlAttribute))

        Attr = XML.CreateNode(System.Xml.XmlNodeType.Attribute, "error", Nothing)
        Attr.Value = ""
        Element.Attributes.Append(CType(Attr, Xml.XmlAttribute))
        XML.DocumentElement.PrependChild(Element)
    End Sub

    ''' <summary>
    '''     Returns True if the tran code exists in the table.
    ''' </summary>
    ''' <remarks>
    '''     This function should be called before trying to reference any of the TranType's properties
    '''     (i.e. - URL(), WebMethod(), etc.)
    ''' </remarks>
    Friend Function TranTypeIsValid() As Boolean
        Try
            If Me.CallCount > 0 Then
                'at least one row exists for tran code, so return True
                Return True
            Else
                'return false if a matching URL and method is not found for the TranType
                Return False
            End If
        Catch ex As Exception
            'exception would occur if tran code table is empty (which should've been caught in the contructor)
            Return False
        End Try
    End Function
    ''' <summary>
    '''     Returns the TranType passed into the Constructor.
    ''' </summary>
    Friend ReadOnly Property TranType() As String
        Get
            Return _TranType
        End Get
    End Property
    ''' <summary>
    '''     Returns the Target's Connect String Key based on the TranType and Index.
    ''' </summary>
    ''' <param name="Index">Record index between zero and record count - 1.</param>
    Friend ReadOnly Property TargetConnectStringKey(ByVal Index As Integer) As String
        Get
            If CallCount > 0 Then
                Return CType(_TranTypes.DefaultView(Index)("TargetConnectStringKey"), String)
            Else
                Return ""
            End If
        End Get
    End Property

    ''' <summary>
    '''     Returns the string to send to the web service.
    ''' </summary>
    ''' <param name="BufferIn">MSMQ Buffer (without the qmsg XML wrappings).</param>
    ''' <param name="Index">Record index between zero and record count - 1.</param>
    ''' <remarks>
    '''     Each TranType record contains enough info to execute a stored procedure and 
    '''     return a record set with the fields formatted for the web service request string. 
    '''     The key value for locating the record is parsed from the buffer by KeyOffset and 
    '''     KeyLen, and passed into the stored proc.
    '''     The recordset returned by SP should be exactly one row. The first column returned is 
    '''     the record's Online Update Status. If the record has already been updated, we don't 
    '''     want to do it again so return -1.
    ''' </remarks>
    Friend Function GetRequest(ByVal BufferIn As String, _
        ByVal Index As Integer) As Integer

        Dim intReturnValue As Integer = -1
        Dim c As DataColumn
        Dim ds As New DataSet
        Dim sb As New System.Text.StringBuilder

        'The first 2 characters of BufferIn is the TranType. Remove it.
        If Len(BufferIn) < 3 Then
            'shouldn't happen, but throw an error if there is no data
            'change the exception action and bubble up exception; caller will create exception
            Throw New Exception("TranTypeUtility.GetRequest Error: Bad BufferIn data [" & BufferIn & "]")
        End If
        BufferIn = Right(BufferIn, Len(BufferIn) - 2)
        'internal copy of buffer in this function is now the buffer without the tran type

        If CType(_TranTypes.DefaultView(Index)("ConnectStringKey"), String) <> "" AndAlso _
            CType(_TranTypes.DefaultView(Index)("SPSelName"), String) <> "" AndAlso _
            CType(_TranTypes.DefaultView(Index)("SPKeyParamName"), String) <> "" AndAlso _
            CType(_TranTypes.DefaultView(Index)("KeyOffset"), Integer) > 0 AndAlso _
            CType(_TranTypes.DefaultView(Index)("KeyLength"), Integer) > 0 Then
            'there is enough info to call a stored proc.

            'get the KeyValue from the buffer, as defined by the KeyOffset and KeyLen
            Dim KeyValue As String = Trim(Mid(BufferIn, _
                CType(_TranTypes.DefaultView(Index)("KeyOffset"), Integer), _
                CType(_TranTypes.DefaultView(Index)("KeyLength"), Integer)))

            If KeyValue = "" Then
                'key is empty, throw an exception indicating the TranType and index)
                'change the exception action and bubble up exception; caller will create exception
                Throw New Exception("TranTypeUtility.GetRequest Error: KeyValue is empty (" & _TranType & ", " & Index.ToString & ")")
            End If

            'call the specified stored proc to return the request string
            Try
                'sp is allowed to return a resultset consisting of one row
                ds = DataObject.ExecuteDataset(CType(_TranTypes.DefaultView(Index)("ConnectStringKey"), String), _
                    CommandType.StoredProcedure, CType(_TranTypes.DefaultView(Index)("SPSelName"), String), _
                    New SqlClient.SqlParameter(CType(_TranTypes.DefaultView(Index)("SPKeyParamName"), String), KeyValue))
            Catch ex As Exception
                'Test Case #5
                'default exception type of QueueWrite is OK so only 
                'bubble up exception and caller will return the message to the queue
                Throw New Exception("TranTypeUtility.GetRequest error building buffer: " & ex.Message)
            End Try

            If ds.Tables.Count = 1 AndAlso ds.Tables(0).Rows.Count = 1 Then
                'record found, return value
                intReturnValue = CType(ds.Tables(0).Rows(0)("SPStatus"), Integer)
                If UCase(Left(CType(ds.Tables(0).Rows(0)("ErrorMsg"), String), 25)) = "RECORD DOES NOT EXIST FOR" Then
                    'record no longer exists, return -1 and modMain will ignore
                    intReturnValue = -1
                End If
            Else
                'record no longer exists, return -1 and modMain will ignore
                intReturnValue = -1
            End If
        Else
            'there isn't enough info to call a stored proc. - throw exception because all cases 
            'should return the buffer from a stored proc.
            sb = Nothing
            ds = Nothing
            'bubble up exception and return to queue. TranType config table error.
            Throw New Exception("TranTypeUtility.GetRequest. Insufficient info for obtaining buffer from database. (" & _TranType & ", " & Index.ToString & ").")
        End If

        Return intReturnValue
    End Function

    ''' <summary>
    '''     Retrieves a message's process attempt value.
    ''' </summary>
    ''' <param name="XML">XMLDocument object that will be checked.</param>
    Friend Function GetAttemptCount(ByRef XML As Xml.XmlDocument) As Integer
        'returns the message's attempt count
        Dim intReturnValue As Integer = 0
        If Me.ProcessNodeExists(XML) = True Then
            'return the attempt attribute value
            intReturnValue = CType(XML.SelectSingleNode("qmsg/process/@attempt").FirstChild.Value, Integer)
        End If
        Return intReturnValue
    End Function
    ''' <summary>
    '''     Returns True if the "status" attribute is greater than the Index, meaning the call was completed. Otherwise, returns false.
    ''' </summary>
    ''' <param name="XML">XMLDocument object that is checked for the "process" node.</param>
    ''' <param name="Index">Record index between zero and record count - 1 indicating a particular web method call.</param>
    Friend Function IsProcessCallCompleted(ByVal XML As Xml.XmlDocument, ByVal Index As Byte) As Boolean
        If Me.ProcessNodeExists(XML) = False Then
            'if the process node doesn't exist, the call has not completed
            Return False
        ElseIf CType(XML.SelectSingleNode("qmsg/process/@status").FirstChild.Value, Byte) > Index Then
            'it exists and its value exceeds the index so this call has completed
            Return True
        Else
            'it exists and its value is less than or equal to the index, so it hasn't completed yet
            Return False
        End If
    End Function
    ''' <summary>
    '''     Updates the VFS database.
    ''' </summary>
    ''' <param name="BufferIn">Message buffer containing the record's key</param>
    ''' <param name="Index">Record index between zero and record count - 1.</param>
    ''' <param name="UpdateValue">Value to use to update the VFS record.</param>
    Friend Sub MessageComplete(ByVal BufferIn As String, ByVal Index As Integer, ByVal UpdateValue As String)
        'update the database if necessary
        'if fails, caller should return to queue

        'The first 2 characters of BufferIn is the TranType. Remove it.
        If Len(BufferIn) < 3 Then
            'shouldn't happen, but throw an error if there is no data 
            Throw New Exception("GetRequest Error: Bad BufferIn data [" & BufferIn & "]")
        End If
        BufferIn = Right(BufferIn, Len(BufferIn) - 2)
        'internal copy of buffer in this function is now the buffer without the tran type

        If CType(_TranTypes.DefaultView(Index)("ConnectStringKey"), String) <> "" AndAlso _
            CType(_TranTypes.DefaultView(Index)("SPSelName"), String) <> "" AndAlso _
            CType(_TranTypes.DefaultView(Index)("SPKeyParamName"), String) <> "" AndAlso _
            CType(_TranTypes.DefaultView(Index)("KeyOffset"), Integer) > 0 AndAlso _
            CType(_TranTypes.DefaultView(Index)("KeyLength"), Integer) > 0 AndAlso _
            CType(_TranTypes.DefaultView(Index)("SPUpdName"), String) <> "" AndAlso _
            CType(_TranTypes.DefaultView(Index)("SPUpdParamName"), String) <> "" Then
            'there is enough info to call a stored proc.

            'get the KeyValue from the buffer, as defined by the KeyOffset and KeyLen
            Dim KeyValue As String = Trim(Mid(BufferIn, _
                CType(_TranTypes.DefaultView(Index)("KeyOffset"), Integer), _
                CType(_TranTypes.DefaultView(Index)("KeyLength"), Integer)))

            If KeyValue = "" Then
                'key is empty, throw an exception indicating the TranType and index)
                Throw New Exception("TranTypeUtil.MessageComplete: KeyValue is empty (" & _TranType & ", " & Index.ToString & ")")
            End If

            'call the specified stored proc to return the request string
            Try
                'sp updates a table in the database
                DataObject.ExecuteNonQuery(CType(_TranTypes.DefaultView(Index)("ConnectStringKey"), String), _
                    CommandType.StoredProcedure, CType(_TranTypes.DefaultView(Index)("SPUpdName"), String), _
                    New SqlClient.SqlParameter(CType(_TranTypes.DefaultView(Index)("SPKeyParamName"), String), KeyValue), _
                    New SqlClient.SqlParameter(CType(_TranTypes.DefaultView(Index)("SPUpdParamName"), String), UpdateValue))
            Catch ex As Exception
                'bubble up error - caller should trap and return to queue because this routine doesn't have the complete message
                Throw New Exception("TranTypeUtility.MessageComplete error updating database: " & ex.Message)
            End Try
        Else
            'there isn't enough info to call a stored proc. - throw exception because all cases 
            'should return the buffer from a stored proc.
            'bubble up exception and caller will create exception
            Throw New Exception("TranTypeUtility.MessageComplete. Insufficient info for obtaining buffer from database (" & _TranType & ", " & Index.ToString & ").")
        End If

    End Sub

    Public Sub New(ByVal TranType As String, ByVal TranTypes As DataTable)
        'pass in the TranType table and filter on the desired tran code
        _TranTypes = CopyDatatable(TranTypes)
        _TranType = TranType
        _TranTypes.DefaultView.RowFilter = "TranType='" & TranType & "'"
        If _TranTypes.DefaultView.Count < 1 Then
            Throw New Exception("Unable to find TranType '" & TranType & "' in list.")
        End If

    End Sub
    ''' <summary>
    '''     Creates a true local copy of the datatable. Since the TranType class can be 
    '''     instantiated by multiple threads and the TranType table passed into the constructor 
    '''     filters the rows, a true local copy is created for true thread safety (since a datatable
    '''     passed ByVal passes the pointer ByVal, not the table object.
    ''' </summary>
    Private Function CopyDatatable(ByVal SrcTable As DataTable) As DataTable
        'create a true copy of the datatable so it behaves properly as a object passed ByVal
        Dim DstTable As New DataTable
        Dim SrcRow As DataRow
        Dim SrcCol As DataColumn

        DstTable = SrcTable.Clone
        For Each SrcRow In SrcTable.Rows
            Dim DstRow As DataRow
            DstRow = DstTable.NewRow()
            For Each SrcCol In SrcTable.Columns
                DstRow(SrcCol.ColumnName) = SrcRow(SrcCol.ColumnName)
            Next
            DstTable.Rows.Add(DstRow)
        Next
        'return 
        Return DstTable

    End Function

    Protected Overrides Sub Finalize()
        MyBase.Finalize()
    End Sub
End Class
