package ftpwriter

// 配置键常量 - 与Java版本保持一致
const (
	KeyProtocol        = "protocol"
	KeyHost           = "host"
	KeyUsername       = "username"
	KeyPassword       = "password"
	KeyPort           = "port"
	KeyTimeout        = "timeout"
	KeyConnectPattern = "connectPattern"
	KeyPath           = "path"

	// 通用writer参数
	KeyFileName       = "fileName"
	KeyWriteMode      = "writeMode"
	KeyFieldDelimiter = "fieldDelimiter"
	KeyEncoding       = "encoding"
	KeyCompress       = "compress"
	KeyNullFormat     = "nullFormat"
	KeyDateFormat     = "dateFormat"
	KeyFileFormat     = "fileFormat"
	KeyHeader         = "header"
	KeySuffix         = "suffix"
)

// 默认值常量
const (
	DefaultFtpPort               = 21
	DefaultSftpPort             = 22
	DefaultTimeout              = 60000  // 毫秒
	DefaultFtpConnectPattern    = "PASV"
	DefaultFieldDelimiter       = ","
	DefaultEncoding             = "UTF-8"
	DefaultNullFormat           = "\\N"
	DefaultFileFormat           = "text"
)