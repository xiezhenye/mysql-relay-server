package mysql

import (
	"encoding/binary"
)

const VERSION_COMMENT = "MySQL Relay Server 0.1.0"

const (
	CLIENT_LONG_PASSWORD                  uint32 = 0x00000001
	CLIENT_FOUND_ROWS                            = 0x00000002
	CLIENT_LONG_FLAG                             = 0x00000004
	CLIENT_CONNECT_WITH_DB                       = 0x00000008
	CLIENT_NO_SCHEMA                             = 0x00000010
	CLIENT_COMPRESS                              = 0x00000020
	CLIENT_ODBC                                  = 0x00000040
	CLIENT_LOCAL_FILES                           = 0x00000080
	CLIENT_IGNORE_SPACE                          = 0x00000100
	CLIENT_PROTOCOL_41                           = 0x00000200
	CLIENT_INTERACTIVE                           = 0x00000400
	CLIENT_SSL                                   = 0x00000800
	CLIENT_IGNORE_SIGPIPE                        = 0x00001000
	CLIENT_TRANSACTIONS                          = 0x00002000
	CLIENT_RESERVED                              = 0x00004000
	CLIENT_SECURE_CONNECTION                     = 0x00008000
	CLIENT_MULTI_STATEMENTS                      = 0x00010000
	CLIENT_MULTI_RESULTS                         = 0x00020000
	CLIENT_PS_MULTI_RESULTS                      = 0x00040000
	CLIENT_PLUGIN_AUTH                           = 0x00080000
	CLIENT_CONNECT_ATTRS                         = 0x00100000
	CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA        = 0x00200000
	CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS          = 0x00400000
	CLIENT_SSL_VERIFY_SERVER_CERT                = 0x40000000
	CLIENT_REMEMBER_OPTIONS                      = 0x80000000
)

const (
	RELAY_CLIENT_CAP   uint32 = CLIENT_PROTOCOL_41 | CLIENT_PLUGIN_AUTH | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
	DEFAULT_SERVER_CAP        = 0x807ff7ff
)

var (
	ENDIAN = binary.LittleEndian
)

const (
	GRP_OK  byte = '\x00'
	GRP_ERR byte = '\xff'
	GRP_EOF byte = '\xfe'
)

const (
	SERVER_STATUS_IN_TRANS             uint16 = 0x0001
	SERVER_STATUS_AUTOCOMMIT                  = 0x0002
	SERVER_MORE_RESULTS_EXISTS                = 0x0008
	SERVER_STATUS_NO_GOOD_INDEX_USED          = 0x0010
	SERVER_STATUS_NO_INDEX_USED               = 0x0020
	SERVER_STATUS_CURSOR_EXISTS               = 0x0040
	SERVER_STATUS_LAST_ROW_SENT               = 0x0080
	SERVER_STATUS_DB_DROPPED                  = 0x0100
	SERVER_STATUS_NO_BACKSLASH_ESCAPES        = 0x0200
	SERVER_STATUS_METADATA_CHANGED            = 0x0400
	SERVER_QUERY_WAS_SLOW                     = 0x0800
	SERVER_PS_OUT_PARAMS                      = 0x1000
	SERVER_STATUS_IN_TRANS_READONLY           = 0x2000
)

const DEFAULT_AUTH_PLUGIN_NAME = "mysql_native_password"

const (
	COM_SLEEP byte = iota
	COM_QUIT
	COM_INIT_DB
	COM_QUERY
	COM_FIELD_LIST
	COM_CREATE_DB
	COM_DROP_DB
	COM_REFRESH
	COM_SHUTDOWN
	COM_STATISTICS
	COM_PROCESS_INFO
	COM_CONNECT
	COM_PROCESS_KILL
	COM_DEBUG
	COM_PING
	COM_TIME
	COM_DELAYED_INSERT
	COM_CHANGE_USER
	COM_BINLOG_DUMP
	COM_TABLE_DUMP
	COM_CONNECT_OUT
	COM_REGISTER_SLAVE
	COM_STMT_PREPARE
	COM_STMT_EXECUTE
	COM_STMT_SEND_LONG_DATA
	COM_STMT_CLOSE
	COM_STMT_RESET
	COM_SET_OPTION
	COM_STMT_FETCH
	COM_DAEMON
	COM_BINLOG_DUMP_GTID
)

var CommandNames = [...]string{"COM_SLEEP", "COM_QUIT", "COM_INIT_DB", "COM_QUERY", "COM_FIELD_LIST", "COM_CREATE_DB", "COM_DROP_DB", "COM_REFRESH", "COM_SHUTDOWN", "COM_STATISTICS", "COM_PROCESS_INFO", "COM_CONNECT", "COM_PROCESS_KILL", "COM_DEBUG", "COM_PING", "COM_TIME", "COM_DELAYED_INSERT", "COM_CHANGE_USER", "COM_BINLOG_DUMP", "COM_TABLE_DUMP", "COM_CONNECT_OUT", "COM_REGISTER_SLAVE", "COM_STMT_PREPARE", "COM_STMT_EXECUTE", "COM_STMT_SEND_LONG_DATA", "COM_STMT_CLOSE", "COM_STMT_RESET", "COM_SET_OPTION", "COM_STMT_FETCH", "COM_DAEMON", "COM_BINLOG_DUMP_GTID"}

const (
	BIG5_CHINESE_CI          byte = 1
	LATIN2_CZECH_CS               = 2
	DEC8_SWEDISH_CI               = 3
	CP850_GENERAL_CI              = 4
	LATIN1_GERMAN1_CI             = 5
	HP8_ENGLISH_CI                = 6
	KOI8R_GENERAL_CI              = 7
	LATIN1_SWEDISH_CI             = 8
	LATIN2_GENERAL_CI             = 9
	SWE7_SWEDISH_CI               = 10
	ASCII_GENERAL_CI              = 11
	UJIS_JAPANESE_CI              = 12
	SJIS_JAPANESE_CI              = 13
	CP1251_BULGARIAN_CI           = 14
	LATIN1_DANISH_CI              = 15
	HEBREW_GENERAL_CI             = 16
	TIS620_THAI_CI                = 18
	EUCKR_KOREAN_CI               = 19
	LATIN7_ESTONIAN_CS            = 20
	LATIN2_HUNGARIAN_CI           = 21
	KOI8U_GENERAL_CI              = 22
	CP1251_UKRAINIAN_CI           = 23
	GB2312_CHINESE_CI             = 24
	GREEK_GENERAL_CI              = 25
	CP1250_GENERAL_CI             = 26
	LATIN2_CROATIAN_CI            = 27
	GBK_CHINESE_CI                = 28
	CP1257_LITHUANIAN_CI          = 29
	LATIN5_TURKISH_CI             = 30
	LATIN1_GERMAN2_CI             = 31
	ARMSCII8_GENERAL_CI           = 32
	UTF8_GENERAL_CI               = 33
	CP1250_CZECH_CS               = 34
	UCS2_GENERAL_CI               = 35
	CP866_GENERAL_CI              = 36
	KEYBCS2_GENERAL_CI            = 37
	MACCE_GENERAL_CI              = 38
	MACROMAN_GENERAL_CI           = 39
	CP852_GENERAL_CI              = 40
	LATIN7_GENERAL_CI             = 41
	LATIN7_GENERAL_CS             = 42
	MACCE_BIN                     = 43
	CP1250_CROATIAN_CI            = 44
	UTF8MB4_GENERAL_CI            = 45
	UTF8MB4_BIN                   = 46
	LATIN1_BIN                    = 47
	LATIN1_GENERAL_CI             = 48
	LATIN1_GENERAL_CS             = 49
	CP1251_BIN                    = 50
	CP1251_GENERAL_CI             = 51
	CP1251_GENERAL_CS             = 52
	MACROMAN_BIN                  = 53
	UTF16_GENERAL_CI              = 54
	UTF16_BIN                     = 55
	UTF16LE_GENERAL_CI            = 56
	CP1256_GENERAL_CI             = 57
	CP1257_BIN                    = 58
	CP1257_GENERAL_CI             = 59
	UTF32_GENERAL_CI              = 60
	UTF32_BIN                     = 61
	UTF16LE_BIN                   = 62
	BINARY                        = 63
	ARMSCII8_BIN                  = 64
	ASCII_BIN                     = 65
	CP1250_BIN                    = 66
	CP1256_BIN                    = 67
	CP866_BIN                     = 68
	DEC8_BIN                      = 69
	GREEK_BIN                     = 70
	HEBREW_BIN                    = 71
	HP8_BIN                       = 72
	KEYBCS2_BIN                   = 73
	KOI8R_BIN                     = 74
	KOI8U_BIN                     = 75
	LATIN2_BIN                    = 77
	LATIN5_BIN                    = 78
	LATIN7_BIN                    = 79
	CP850_BIN                     = 80
	CP852_BIN                     = 81
	SWE7_BIN                      = 82
	UTF8_BIN                      = 83
	BIG5_BIN                      = 84
	EUCKR_BIN                     = 85
	GB2312_BIN                    = 86
	GBK_BIN                       = 87
	SJIS_BIN                      = 88
	TIS620_BIN                    = 89
	UCS2_BIN                      = 90
	UJIS_BIN                      = 91
	GEOSTD8_GENERAL_CI            = 92
	GEOSTD8_BIN                   = 93
	LATIN1_SPANISH_CI             = 94
	CP932_JAPANESE_CI             = 95
	CP932_BIN                     = 96
	EUCJPMS_JAPANESE_CI           = 97
	EUCJPMS_BIN                   = 98
	CP1250_POLISH_CI              = 99
	UTF16_UNICODE_CI              = 101
	UTF16_ICELANDIC_CI            = 102
	UTF16_LATVIAN_CI              = 103
	UTF16_ROMANIAN_CI             = 104
	UTF16_SLOVENIAN_CI            = 105
	UTF16_POLISH_CI               = 106
	UTF16_ESTONIAN_CI             = 107
	UTF16_SPANISH_CI              = 108
	UTF16_SWEDISH_CI              = 109
	UTF16_TURKISH_CI              = 110
	UTF16_CZECH_CI                = 111
	UTF16_DANISH_CI               = 112
	UTF16_LITHUANIAN_CI           = 113
	UTF16_SLOVAK_CI               = 114
	UTF16_SPANISH2_CI             = 115
	UTF16_ROMAN_CI                = 116
	UTF16_PERSIAN_CI              = 117
	UTF16_ESPERANTO_CI            = 118
	UTF16_HUNGARIAN_CI            = 119
	UTF16_SINHALA_CI              = 120
	UTF16_GERMAN2_CI              = 121
	UTF16_CROATIAN_CI             = 122
	UTF16_UNICODE_520_CI          = 123
	UTF16_VIETNAMESE_CI           = 124
	UCS2_UNICODE_CI               = 128
	UCS2_ICELANDIC_CI             = 129
	UCS2_LATVIAN_CI               = 130
	UCS2_ROMANIAN_CI              = 131
	UCS2_SLOVENIAN_CI             = 132
	UCS2_POLISH_CI                = 133
	UCS2_ESTONIAN_CI              = 134
	UCS2_SPANISH_CI               = 135
	UCS2_SWEDISH_CI               = 136
	UCS2_TURKISH_CI               = 137
	UCS2_CZECH_CI                 = 138
	UCS2_DANISH_CI                = 139
	UCS2_LITHUANIAN_CI            = 140
	UCS2_SLOVAK_CI                = 141
	UCS2_SPANISH2_CI              = 142
	UCS2_ROMAN_CI                 = 143
	UCS2_PERSIAN_CI               = 144
	UCS2_ESPERANTO_CI             = 145
	UCS2_HUNGARIAN_CI             = 146
	UCS2_SINHALA_CI               = 147
	UCS2_GERMAN2_CI               = 148
	UCS2_CROATIAN_CI              = 149
	UCS2_UNICODE_520_CI           = 150
	UCS2_VIETNAMESE_CI            = 151
	UCS2_GENERAL_MYSQL500_CI      = 159
	UTF32_UNICODE_CI              = 160
	UTF32_ICELANDIC_CI            = 161
	UTF32_LATVIAN_CI              = 162
	UTF32_ROMANIAN_CI             = 163
	UTF32_SLOVENIAN_CI            = 164
	UTF32_POLISH_CI               = 165
	UTF32_ESTONIAN_CI             = 166
	UTF32_SPANISH_CI              = 167
	UTF32_SWEDISH_CI              = 168
	UTF32_TURKISH_CI              = 169
	UTF32_CZECH_CI                = 170
	UTF32_DANISH_CI               = 171
	UTF32_LITHUANIAN_CI           = 172
	UTF32_SLOVAK_CI               = 173
	UTF32_SPANISH2_CI             = 174
	UTF32_ROMAN_CI                = 175
	UTF32_PERSIAN_CI              = 176
	UTF32_ESPERANTO_CI            = 177
	UTF32_HUNGARIAN_CI            = 178
	UTF32_SINHALA_CI              = 179
	UTF32_GERMAN2_CI              = 180
	UTF32_CROATIAN_CI             = 181
	UTF32_UNICODE_520_CI          = 182
	UTF32_VIETNAMESE_CI           = 183
	UTF8_UNICODE_CI               = 192
	UTF8_ICELANDIC_CI             = 193
	UTF8_LATVIAN_CI               = 194
	UTF8_ROMANIAN_CI              = 195
	UTF8_SLOVENIAN_CI             = 196
	UTF8_POLISH_CI                = 197
	UTF8_ESTONIAN_CI              = 198
	UTF8_SPANISH_CI               = 199
	UTF8_SWEDISH_CI               = 200
	UTF8_TURKISH_CI               = 201
	UTF8_CZECH_CI                 = 202
	UTF8_DANISH_CI                = 203
	UTF8_LITHUANIAN_CI            = 204
	UTF8_SLOVAK_CI                = 205
	UTF8_SPANISH2_CI              = 206
	UTF8_ROMAN_CI                 = 207
	UTF8_PERSIAN_CI               = 208
	UTF8_ESPERANTO_CI             = 209
	UTF8_HUNGARIAN_CI             = 210
	UTF8_SINHALA_CI               = 211
	UTF8_GERMAN2_CI               = 212
	UTF8_CROATIAN_CI              = 213
	UTF8_UNICODE_520_CI           = 214
	UTF8_VIETNAMESE_CI            = 215
	UTF8_GENERAL_MYSQL500_CI      = 223
	UTF8MB4_UNICODE_CI            = 224
	UTF8MB4_ICELANDIC_CI          = 225
	UTF8MB4_LATVIAN_CI            = 226
	UTF8MB4_ROMANIAN_CI           = 227
	UTF8MB4_SLOVENIAN_CI          = 228
	UTF8MB4_POLISH_CI             = 229
	UTF8MB4_ESTONIAN_CI           = 230
	UTF8MB4_SPANISH_CI            = 231
	UTF8MB4_SWEDISH_CI            = 232
	UTF8MB4_TURKISH_CI            = 233
	UTF8MB4_CZECH_CI              = 234
	UTF8MB4_DANISH_CI             = 235
	UTF8MB4_LITHUANIAN_CI         = 236
	UTF8MB4_SLOVAK_CI             = 237
	UTF8MB4_SPANISH2_CI           = 238
	UTF8MB4_ROMAN_CI              = 239
	UTF8MB4_PERSIAN_CI            = 240
	UTF8MB4_ESPERANTO_CI          = 241
	UTF8MB4_HUNGARIAN_CI          = 242
	UTF8MB4_SINHALA_CI            = 243
	UTF8MB4_GERMAN2_CI            = 244
	UTF8MB4_CROATIAN_CI           = 245
	UTF8MB4_UNICODE_520_CI        = 246
	UTF8MB4_VIETNAMESE_CI         = 247
)

const (
	MYSQL_TYPE_DECIMAL     byte = 0x00
	MYSQL_TYPE_TINY             = 0x01
	MYSQL_TYPE_SHORT            = 0x02
	MYSQL_TYPE_LONG             = 0x03
	MYSQL_TYPE_FLOAT            = 0x04
	MYSQL_TYPE_DOUBLE           = 0x05
	MYSQL_TYPE_NULL             = 0x06
	MYSQL_TYPE_TIMESTAMP        = 0x07
	MYSQL_TYPE_LONGLONG         = 0x08
	MYSQL_TYPE_INT24            = 0x09
	MYSQL_TYPE_DATE             = 0x0a
	MYSQL_TYPE_TIME             = 0x0b
	MYSQL_TYPE_DATETIME         = 0x0c
	MYSQL_TYPE_YEAR             = 0x0d
	MYSQL_TYPE_NEWDATE          = 0x0e
	MYSQL_TYPE_VARCHAR          = 0x0f
	MYSQL_TYPE_BIT              = 0x10
	MYSQL_TYPE_TIMESTAMP2       = 0x11
	MYSQL_TYPE_DATETIME2        = 0x12
	MYSQL_TYPE_TIME2            = 0x13
	MYSQL_TYPE_NEWDECIMAL       = 0xf6
	MYSQL_TYPE_ENUM             = 0xf7
	MYSQL_TYPE_SET              = 0xf8
	MYSQL_TYPE_TINY_BLOB        = 0xf9
	MYSQL_TYPE_MEDIUM_BLOB      = 0xfa
	MYSQL_TYPE_LONG_BLOB        = 0xfb
	MYSQL_TYPE_BLOB             = 0xfc
	MYSQL_TYPE_VAR_STRING       = 0xfd
	MYSQL_TYPE_STRING           = 0xfe
	MYSQL_TYPE_GEOMETRY         = 0xff
)

var MysqlTypeSize = map[byte]int{
	MYSQL_TYPE_NULL:        0,
	MYSQL_TYPE_STRING:      -1,
	MYSQL_TYPE_VARCHAR:     -1,
	MYSQL_TYPE_VAR_STRING:  -1,
	MYSQL_TYPE_ENUM:        -1,
	MYSQL_TYPE_SET:         -1,
	MYSQL_TYPE_LONG_BLOB:   -1,
	MYSQL_TYPE_MEDIUM_BLOB: -1,
	MYSQL_TYPE_BLOB:        -1,
	MYSQL_TYPE_TINY_BLOB:   -1,
	MYSQL_TYPE_GEOMETRY:    -1,
	MYSQL_TYPE_BIT:         -1,
	MYSQL_TYPE_DECIMAL:     -1,
	MYSQL_TYPE_NEWDECIMAL:  -1,
	MYSQL_TYPE_LONGLONG:    8,
	MYSQL_TYPE_LONG:        4,
	MYSQL_TYPE_INT24:       4,
	MYSQL_TYPE_SHORT:       2,
	MYSQL_TYPE_YEAR:        2,
	MYSQL_TYPE_TINY:        1,
	MYSQL_TYPE_DOUBLE:      8,
	MYSQL_TYPE_FLOAT:       4,
	MYSQL_TYPE_DATE:        -1,
	MYSQL_TYPE_DATETIME:    -1,
	MYSQL_TYPE_TIME:        -1,
	MYSQL_TYPE_TIMESTAMP:   -1,
	MYSQL_TYPE_TIME2:       -1,
	MYSQL_TYPE_DATETIME2:   -1,
	MYSQL_TYPE_TIMESTAMP2:  -1,
}
