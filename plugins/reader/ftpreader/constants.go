package ftpreader

// 配置键常量
const (
	KeyProtocol            = "protocol"
	KeyHost               = "host"
	KeyUsername           = "username"
	KeyPassword           = "password"
	KeyPort               = "port"
	KeyTimeout            = "timeout"
	KeyConnectPattern     = "connectPattern"
	KeyPath               = "path"
	KeyMaxTraversalLevel  = "maxTraversalLevel"
	KeyFieldDelimiter     = "fieldDelimiter"
	KeyEncoding           = "encoding"
	KeyCompress           = "compress"
	KeySkipHeader         = "skipHeader"
	KeyNullFormat         = "nullFormat"
	KeyCsvReaderConfig    = "csvReaderConfig"
	KeyColumn             = "column"
)

// 默认值常量
const (
	DefaultFtpPort               = 21
	DefaultSftpPort             = 22
	DefaultTimeout              = 60000  // 毫秒
	DefaultMaxTraversalLevel    = 100
	DefaultFtpConnectPattern    = "PASV"
	DefaultFieldDelimiter       = ","
	DefaultEncoding             = "UTF-8"
	DefaultNullFormat           = "\\N"
)

// 内部使用常量
const (
	SourceFiles = "sourceFiles"
)