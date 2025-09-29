package ossreader

// 配置项常量，与Java版本保持一致
const (
	// OSS连接配置
	KeyEndpoint   = "endpoint"
	KeyAccessId   = "accessId"
	KeyAccessKey  = "accessKey"
	KeyBucket     = "bucket"
	KeyObject     = "object"
	KeyCname      = "cname"

	// 文件处理配置
	KeyColumn        = "column"
	KeyFieldDelimiter = "fieldDelimiter"
	KeyCompress      = "compress"
	KeyEncoding      = "encoding"
	KeySkipHeader    = "skipHeader"
	KeyNullFormat    = "nullFormat"
	KeyDateFormat    = "dateFormat"
	KeyFileFormat    = "fileFormat"

	// 高级配置
	KeySuccessOnNoObject = "successOnNoObject"
	KeyProxyHost         = "proxyHost"
	KeyProxyPort         = "proxyPort"
	KeyProxyUsername     = "proxyUsername"
	KeyProxyPassword     = "proxyPassword"
	KeyProxyDomain       = "proxyDomain"
	KeyProxyWorkstation  = "proxyWorkstation"

	// 分片配置
	KeyBalanceThreshold = "balanceThreshold"

	// 默认值
	DefaultFileFormat      = "text"
	DefaultFieldDelimiter  = ","
	DefaultEncoding        = "UTF-8"
	DefaultNullFormat      = "\\N"
	DefaultCompress        = ""
	DefaultSkipHeader      = false
	DefaultSuccessOnNoObject = false
	DefaultBalanceThreshold  = 0.3

	// 分片阈值常量
	SingleFileSplitThresholdInSize = 1024 * 1024 * 1024 // 1GB
)