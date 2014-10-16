package server

import (
    "time"
    "strconv"
    "fmt"
    "mysql_relay/mysql"
)

func (peer *Peer) onCmdQuery(cmdPacket *mysql.BaseCommandPacket) (err error) {
	query := string(peer.Buffer[1:cmdPacket.PacketLength])
	fmt.Println(query)
	query = NormalizeSpecialQuery(query)
	switch query {
	case "select @@version_comment limit 1":
		return onSqlVersionComment(peer)
	case "show variables like 'server_id'":
		return onSqlServerId(peer)
	case "show variables like 'server_uuid'":
		return onSqlServerUuid(peer)
	case "select unix_timestamp()":
		return onSqlUnixTimestamp(peer)
	case "select version()":
		return onSqlVersion(peer)
	case "set @master_binlog_checksum='none'":
		return peer.SendOk(cmdPacket.PacketSeq + 1)
	}
	errPacket := mysql.BuildErrPacket(mysql.ER_NOT_SUPPORTED_YET, "this")
	errPacket.PacketSeq = cmdPacket.PacketSeq+1
	err = mysql.WritePacketTo(&errPacket, peer.Conn, peer.Buffer[:])
	return
}

func onSqlVersionComment(peer *Peer) (err error) {
	//select @@version_comment limit 1
	cols := [1]mysql.ColumnDefinition{
		{
			Catalog: "def",
			Name: "@@version_comment",
			Decimals: 127,
			CharacterSet: mysql.LATIN1_SWEDISH_CI,
			Type: mysql.MYSQL_TYPE_VAR_STRING,
			ColumnLength: 28,
		},
	}
	cursor := mysql.Cursor {
		Columns: cols[:],
		ReadWriter: peer.Conn,
		Buffer: peer.Buffer[:],
	}
	err = cursor.BeginWrite()
	if err != nil {
		return
	}
	cursor.Rows <- mysql.ResultRow{ Values: []mysql.Value{
		{Value: mysql.VERSION_COMMENT, IsNull: false} ,
	}}
	close(cursor.Rows)
	return
}

func onSqlVersion(peer *Peer) (err error) {
	//select version()
	cols := [1]mysql.ColumnDefinition{
		{
			Catalog: "def",
			Name: "version()",
			Decimals: 127,
			CharacterSet: mysql.LATIN1_SWEDISH_CI,
			Type: mysql.MYSQL_TYPE_VAR_STRING,
			ColumnLength: 28,
		},
	}
	cursor := mysql.Cursor {
		Columns: cols[:],
		ReadWriter: peer.Conn,
		Buffer: peer.Buffer[:],
	}
	err = cursor.BeginWrite()
	if err != nil {
		return
	}
	cursor.Rows <- mysql.ResultRow{ Values: []mysql.Value{
		{Value: "5.6.19-log", IsNull: false} ,
	}}
	close(cursor.Rows)
	return
}

func onSqlServerId(peer *Peer) (err error) {
	//show variables like 'server_id'
	cols := [2]mysql.ColumnDefinition{
		{
			Catalog: "def",
			Schema: "information_schema",
			Table: "VARIABLES",
			OrgTable: "VARIABLES",
			Name: "Variable_name",
			OrgName: "VARIABLE_NAME",
			Decimals: 0,
			CharacterSet: mysql.LATIN1_SWEDISH_CI,
			Type: mysql.MYSQL_TYPE_VAR_STRING,
			Flags: mysql.SERVER_STATUS_IN_TRANS,
			ColumnLength: 192,
		},
		{
			Catalog: "def",
			Schema: "information_schema",
			Table: "VARIABLES",
			OrgTable: "VARIABLES",
			Name: "Variable_value",
			OrgName: "VARIABLE_VALUE",
			Decimals: 0,
			CharacterSet: mysql.LATIN1_SWEDISH_CI,
			Type: mysql.MYSQL_TYPE_VAR_STRING,
			Flags: 0,
			ColumnLength: 3072,
		},
	}
	cursor := mysql.Cursor {
		Columns: cols[:],
		ReadWriter: peer.Conn,
		Buffer: peer.Buffer[:],
	}
	err = cursor.BeginWrite()
	if err != nil {
		return
	}
	cursor.Rows <- mysql.ResultRow{ Values: []mysql.Value{
		{Value: "server_id", IsNull: false} ,
		{Value: "2", IsNull: false} ,
	}}
	close(cursor.Rows)
	return
}

func onSqlServerUuid(peer *Peer) (err error) {
	//show variables like 'server_uuid'
	cols := [2]mysql.ColumnDefinition{
		{
			Catalog: "def",
			Schema: "information_schema",
			Table: "VARIABLES",
			OrgTable: "VARIABLES",
			Name: "Variable_name",
			OrgName: "VARIABLE_NAME",
			Decimals: 0,
			CharacterSet: mysql.LATIN1_SWEDISH_CI,
			Type: mysql.MYSQL_TYPE_VAR_STRING,
			Flags: mysql.SERVER_STATUS_IN_TRANS,
			ColumnLength: 192,
		},
		{
			Catalog: "def",
			Schema: "information_schema",
			Table: "VARIABLES",
			OrgTable: "VARIABLES",
			Name: "Variable_value",
			OrgName: "VARIABLE_VALUE",
			Decimals: 0,
			CharacterSet: mysql.LATIN1_SWEDISH_CI,
			Type: mysql.MYSQL_TYPE_VAR_STRING,
			Flags: 0,
			ColumnLength: 3072,
		},
	}
	cursor := mysql.Cursor {
		Columns: cols[:],
		ReadWriter: peer.Conn,
		Buffer: peer.Buffer[:],
	}
	err = cursor.BeginWrite()
	if err != nil {
		return
	}
	cursor.Rows <- mysql.ResultRow{ Values: []mysql.Value{
		{Value: "server_uuid", IsNull: false} ,
		{Value: peer.Server.Config.Server.Uuid, IsNull: false} ,
	}}
	close(cursor.Rows)
	return
}

func onSqlUnixTimestamp(peer *Peer) (err error) {
	//select unix_timestamp()
	cols := [1]mysql.ColumnDefinition{
		{
			Catalog: "def",
			Name: "unix_timestamp()",
			Decimals: 127,
			CharacterSet: mysql.LATIN1_SWEDISH_CI,
			Type: mysql.MYSQL_TYPE_LONGLONG,
			ColumnLength: 11,
		},
	}
	cursor := mysql.Cursor {
		Columns: cols[:],
		ReadWriter: peer.Conn,
		Buffer: peer.Buffer[:],
	}
	err = cursor.BeginWrite()
	if err != nil {
		return
	}
	cursor.Rows <- mysql.ResultRow{ Values: []mysql.Value{
		{Value: strconv.FormatInt(time.Now().Unix(), 10), IsNull: false} ,
	}}
	close(cursor.Rows)
	return
}


func (peer *Peer) onCmdPing(cmdPacket *mysql.BaseCommandPacket) (err error) {
	return
}

func (peer *Peer) onCmdUnknown(cmdPacket *mysql.BaseCommandPacket) (err error) {
	errPacket := mysql.BuildErrPacket(mysql.ER_UNKNOWN_COM_ERROR)
	errPacket.PacketSeq = cmdPacket.PacketSeq+1
	err = mysql.WritePacketTo(&errPacket, peer.Conn, peer.Buffer[:])
	return
}
