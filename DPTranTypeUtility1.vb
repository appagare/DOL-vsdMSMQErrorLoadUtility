Option Strict On
Option Explicit On 

''' <summary>
'''     This is a helper class that handles all of the DPTranType specific tasks.
''' </summary>
Friend Class DPTranTypeUtility
    Private DataObject As WA.DOL.Data.SqlHelper 'common Data object
    Private _DPTranType As String = ""
    Private _DPTranTypes As New DataTable
    Private _XML As New Xml.XmlDocument

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
    '''     Checks for valid buffer data including valid TranType and non blank TranKey
    ''' </summary>
    Friend Sub TranIsValid(ByVal BufferIn As String)

        If Len(BufferIn) < 3 Then
            'shouldn't happen, but throw an error if there is no data 
            Throw New Exception("GetRequest Error: Bad BufferIn data [" & BufferIn & "]")
        End If

        If _DPTranType = "" Then
            'no rows exist for TranType
            Throw New Exception("Unrecognized TranType: [" & BufferIn & "]")
        End If

        BufferIn = Right(BufferIn, Len(BufferIn) - 2)
        'internal copy of buffer in this function is now the buffer without the tran type

        'get the KeyValue from the buffer, as defined by the KeyOffset and KeyLen
        Dim KeyValue As String = Trim(Mid(BufferIn, _
            CType(_DPTranTypes.DefaultView(0)("KeyOffset"), Integer), _
            CType(_DPTranTypes.DefaultView(0)("KeyLength"), Integer)))

        If KeyValue = "" Then
            'key is empty, throw an exception indicating the TranType)
            Throw New Exception("DPTranTypeUtil.MessageComplete: KeyValue is empty " & _DPTranType)
        End If
    End Sub

    ''' <summary>
    '''     Returns the TranType passed into the Constructor.
    ''' </summary>
    Friend ReadOnly Property DPTranType() As String
        Get
            Return _DPTranType
        End Get
    End Property

    ''' <summary>
    '''     Returns True if the "process" node of the xml document exists. Otherwise, returns false.
    ''' </summary>
    ''' <param name="XML">XMLDocument object that is checked for the "process" node.</param>
    Friend Function IsProcessCallCompleted(ByVal XML As Xml.XmlDocument) As Boolean
        If Me.ProcessNodeExists(XML) = False Then
            'if the process node doesn't exist, the call has not completed
            Return False
        ElseIf CType(XML.SelectSingleNode("qmsg/process/@status").FirstChild.Value, Byte) = 1 Then
            'it exists and its value is 1 so this call has completed
            Return True
        Else
            'it exists and its value is less than 1, so it hasn't completed yet
            Return False
        End If
    End Function

    Friend Sub MessageComplete(ByVal BufferIn As String, ByVal ClientId As Decimal)
        'update the database if necessary
        'if fails, caller should return to queue

        'The first 2 characters of BufferIn is the TranType. Remove it.
        BufferIn = Right(BufferIn, Len(BufferIn) - 2)
        'internal copy of buffer in this function is now the buffer without the tran type

        If CType(_DPTranTypes.DefaultView(0)("ConnectStringKey"), String) <> "" AndAlso _
            CType(_DPTranTypes.DefaultView(0)("SPSelName"), String) <> "" AndAlso _
            CType(_DPTranTypes.DefaultView(0)("SPKeyParamName"), String) <> "" AndAlso _
            CType(_DPTranTypes.DefaultView(0)("KeyOffset"), Integer) > 0 AndAlso _
            CType(_DPTranTypes.DefaultView(0)("KeyLength"), Integer) > 0 AndAlso _
            CType(_DPTranTypes.DefaultView(0)("SPUpdName"), String) <> "" Then
            'there is enough info to call a stored proc.

            'get the KeyValue from the buffer, as defined by the KeyOffset and KeyLen
            Dim KeyValue As String = Trim(Mid(BufferIn, _
                CType(_DPTranTypes.DefaultView(0)("KeyOffset"), Integer), _
                CType(_DPTranTypes.DefaultView(0)("KeyLength"), Integer)))

            'call the specified stored proc to return the request string
            Try
                'sp updates a table in the database
                DataObject.ExecuteNonQuery(CType(_DPTranTypes.DefaultView(0)("ConnectStringKey"), String), _
                    CommandType.StoredProcedure, CType(_DPTranTypes.DefaultView(0)("SPUpdName"), String), _
                    New SqlClient.SqlParameter(CType(_DPTranTypes.DefaultView(0)("SPKeyParamName"), String), KeyValue), _
                    New SqlClient.SqlParameter("@ClientId", ClientId))
            Catch ex As Exception
                'bubble up error - caller should trap and return to queue because this routine doesn't have the complete message
                Throw New Exception("DPTranTypeUtility.MessageComplete error updating database: " & ex.Message)
            End Try
        Else
            'there isn't enough info to call a stored proc. - throw exception because all cases 
            'should return the buffer from a stored proc.
            'bubble up exception and caller will create exception
            Throw New Exception("DPTranTypeUtility.MessageComplete. Insufficient info for obtaining buffer from database " & _DPTranType)
        End If

    End Sub

    Public Sub New(ByVal DPTranType As String, ByVal DPTranTypes As DataTable)
        'pass in the TranType table and filter on the desired tran code
        _DPTranTypes = CopyDatatable(DPTranTypes)
        _DPTranType = DPTranType
        _DPTranTypes.DefaultView.RowFilter = "TranType='" & DPTranType & "'"

    End Sub

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

