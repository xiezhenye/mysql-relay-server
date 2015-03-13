package server

import (
	"fmt"
	"mysql_relay/mysql"
	"strconv"
	"strings"
	"time"
)

/*
sql slave executed:

SELECT UNIX_TIMESTAMP();
SHOW VARIABLES LIKE 'SERVER_ID';
SET @master_heartbeat_period= 1799999979520;
SET @master_binlog_checksum= @@global.binlog_checksum;
SELECT @master_binlog_checksum;
SELECT @@GLOBAL.GTID_MODE;
SHOW VARIABLES LIKE 'SERVER_UUID';
 =>
select unix_timestamp()                                         done.
show variables like 'server_id'                                 done
set @master_heartbeat_period=1799999979520
set @master_binlog_checksum=@@global.binlog_checksum            done
select @master_binlog_checksum                                  done
select @@global.gtid_mode                                       done
show variables like 'server_uuid'                               done.

#HY000Unknown system variable 'binlog_checksum'
#HY000Unknown system variable 'GTID_MODE'

SELECT VERSION()  done.
*/
func (peer *Peer) onCmdQuery(cmdPacket *mysql.BaseCommandPacket) (err error) {
	query := string(peer.Buffer[1:cmdPacket.PacketLength])
	fmt.Println(query)
	query = NormalizeSpecialQuery(query)
	switch query {
	case "select @@version_comment limit 1":
		return selectVar(peer, "@@version_comment", mysql.MYSQL_TYPE_VAR_STRING, mysql.StringValue(mysql.VERSION_COMMENT))

	case "show variables like 'server_id'":
		return showSingleVar(peer, "server_id", strconv.Itoa(int(peer.Server.Config.Server.ServerId)))

	case "show variables like 'server_uuid'":
		return showSingleVar(peer, "server_uuid", peer.Server.Config.Server.Uuid)

	case "select unix_timestamp()":
		return selectVar(peer, "unix_timestamp()", mysql.MYSQL_TYPE_LONGLONG, mysql.StringValue(strconv.FormatInt(time.Now().Unix(), 10)))

	case "select version()":
		return selectVar(peer, "version()", mysql.MYSQL_TYPE_VAR_STRING, mysql.StringValue(peer.Server.Config.Server.Version))

	case "set @master_binlog_checksum='none'":
		return peer.SendOk(cmdPacket.PacketSeq + 1)

	case "set @master_binlog_checksum=@@global.binlog_checksum":
		return peer.SendOk(cmdPacket.PacketSeq + 1)
	case "select @@global.gtid_mode":
		return selectVar(peer, "@@global.gtid_mode", mysql.MYSQL_TYPE_VAR_STRING, mysql.StringValue("OFF"))

	case "select @master_binlog_checksum":
		return selectVar(peer, "@master_binlog_checksum", mysql.MYSQL_TYPE_VAR_STRING, mysql.StringValue("CRC32"))
	}
	if strings.HasPrefix(query, "set @master_heartbeat_period=") {
		return peer.SendOk(cmdPacket.PacketSeq + 1)
	} else if strings.HasPrefix(query, "set @slave_uuid=") {
		return peer.SendOk(cmdPacket.PacketSeq + 1)
	} else if strings.HasPrefix(query, "set names ") {
		return peer.SendOk(cmdPacket.PacketSeq + 1)
	}
	errPacket := mysql.BuildErrPacket(mysql.ER_NOT_SUPPORTED_YET, "this")
	errPacket.PacketSeq = cmdPacket.PacketSeq + 1
	err = mysql.WritePacketTo(&errPacket, peer.Conn, peer.Buffer[:])

	return
}

func selectVar(peer *Peer, name string, columnType byte, value mysql.Value) (err error) {
	var length uint32
	if value.IsNull {
		length = 5
	} else {
		length = uint32(len(value.Value)) + 1
	}
	cols := [1]mysql.ColumnDefinition{
		{
			Catalog:      "def",
			Name:         name,
			Decimals:     127,
			CharacterSet: mysql.LATIN1_SWEDISH_CI,
			Type:         columnType,
			ColumnLength: length,
		},
	}
	cursor := mysql.Cursor{
		Columns:    cols[:],
		ReadWriter: peer.Conn,
		Buffer:     peer.Buffer[:],
	}
	err = cursor.BeginWrite()
	if err != nil {
		return
	}
	cursor.Rows <- mysql.ResultRow{Values: []mysql.Value{
		value,
	}}
	close(cursor.Rows)
	return
}

func selectMasterBinlogChecksum(peer *Peer) (err error) {
	cols := [1]mysql.ColumnDefinition{
		{
			Catalog:      "def",
			Name:         "@master_binlog_checksum",
			Decimals:     31,
			CharacterSet: mysql.BINARY,
			Type:         mysql.MYSQL_TYPE_LONG_BLOB,
			ColumnLength: 16777216,
			Flags:        mysql.COL_DEF_BINARY,
		},
	}
	cursor := mysql.Cursor{
		Columns:    cols[:],
		ReadWriter: peer.Conn,
		Buffer:     peer.Buffer[:],
	}
	err = cursor.BeginWrite()
	if err != nil {
		return
	}
	cursor.Rows <- mysql.ResultRow{Values: []mysql.Value{
		mysql.StringValue("CRC32"),
	}}
	close(cursor.Rows)
	return
}

func showSingleVar(peer *Peer, name string, value string) (err error) {
	//show variables like 'server_id'
	cols := [2]mysql.ColumnDefinition{
		{
			Catalog:      "def",
			Schema:       "information_schema",
			Table:        "VARIABLES",
			OrgTable:     "VARIABLES",
			Name:         "Variable_name",
			OrgName:      "VARIABLE_NAME",
			Decimals:     0,
			CharacterSet: mysql.LATIN1_SWEDISH_CI,
			Type:         mysql.MYSQL_TYPE_VAR_STRING,
			Flags:        mysql.SERVER_STATUS_IN_TRANS,
			ColumnLength: uint32(len(name)) + 1, //192
		},
		{
			Catalog:      "def",
			Schema:       "information_schema",
			Table:        "VARIABLES",
			OrgTable:     "VARIABLES",
			Name:         "Variable_value",
			OrgName:      "VARIABLE_VALUE",
			Decimals:     0,
			CharacterSet: mysql.LATIN1_SWEDISH_CI,
			Type:         mysql.MYSQL_TYPE_VAR_STRING,
			Flags:        0,
			ColumnLength: uint32(len(value)) + 1, //3072
		},
	}
	cursor := mysql.Cursor{
		Columns:    cols[:],
		ReadWriter: peer.Conn,
		Buffer:     peer.Buffer[:],
	}
	err = cursor.BeginWrite()
	if err != nil {
		return
	}
	cursor.Rows <- mysql.ResultRow{Values: []mysql.Value{
		{Value: name, IsNull: false},
		{Value: value, IsNull: false},
	}}
	close(cursor.Rows)
	return
}

func (peer *Peer) onCmdPing(cmdPacket *mysql.BaseCommandPacket) (err error) {
	// TODO: implements ping
	return
}

func (peer *Peer) onCmdUnknown(cmdPacket *mysql.BaseCommandPacket) (err error) {
	fmt.Printf("Unknown Command! %s\n", mysql.CommandNames[cmdPacket.CommandType()])
	errPacket := mysql.BuildErrPacket(mysql.ER_UNKNOWN_COM_ERROR)
	errPacket.PacketSeq = cmdPacket.PacketSeq + 1
	err = mysql.WritePacketTo(&errPacket, peer.Conn, peer.Buffer[:])
	return
}
