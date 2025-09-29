package ftpreader

import (
	"fmt"
	"io"
	"net"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jlaffaye/ftp"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// FtpHelper 接口定义了FTP和SFTP操作的通用方法
type FtpHelper interface {
	LoginFtpServer(host, username, password string, port, timeout int, connectPattern string) error
	LogoutFtpServer() error
	GetAllFiles(paths []string, currentLevel, maxLevel int) ([]string, error)
	GetInputStream(fileName string) (io.ReadCloser, error)
}

// StandardFtpHelper 实现标准FTP协议
type StandardFtpHelper struct {
	ftpClient *ftp.ServerConn
}

func NewStandardFtpHelper() *StandardFtpHelper {
	return &StandardFtpHelper{}
}

func (h *StandardFtpHelper) LoginFtpServer(host, username, password string, port, timeout int, connectPattern string) error {
	address := net.JoinHostPort(host, strconv.Itoa(port))

	// 创建FTP客户端并连接
	ftpClient, err := ftp.Dial(address, ftp.DialWithTimeout(time.Duration(timeout)*time.Millisecond))
	if err != nil {
		return fmt.Errorf("failed to connect to FTP server %s: %v", address, err)
	}

	// 登录
	err = ftpClient.Login(username, password)
	if err != nil {
		ftpClient.Quit()
		return fmt.Errorf("failed to login to FTP server: %v", err)
	}

	h.ftpClient = ftpClient
	return nil
}

func (h *StandardFtpHelper) LogoutFtpServer() error {
	if h.ftpClient != nil {
		return h.ftpClient.Quit()
	}
	return nil
}

func (h *StandardFtpHelper) GetAllFiles(paths []string, currentLevel, maxLevel int) ([]string, error) {
	if currentLevel > maxLevel {
		return nil, fmt.Errorf("maximum traversal level exceeded: %d", maxLevel)
	}

	var allFiles []string

	for _, pathPattern := range paths {
		files, err := h.getFilesFromPath(pathPattern, currentLevel, maxLevel)
		if err != nil {
			return nil, fmt.Errorf("failed to get files from path %s: %v", pathPattern, err)
		}
		allFiles = append(allFiles, files...)
	}

	return allFiles, nil
}

func (h *StandardFtpHelper) getFilesFromPath(pathPattern string, currentLevel, maxLevel int) ([]string, error) {
	// 检查路径是否包含通配符
	if strings.Contains(pathPattern, "*") {
		return h.expandWildcardPath(pathPattern, currentLevel, maxLevel)
	}

	// 直接路径处理
	entries, err := h.ftpClient.List(pathPattern)
	if err != nil {
		// 如果List失败，尝试作为文件处理
		return []string{pathPattern}, nil
	}

	var files []string
	if len(entries) == 1 && entries[0].Type == ftp.EntryTypeFile {
		// 单个文件
		files = append(files, pathPattern)
	} else {
		// 目录或多个条目
		for _, entry := range entries {
			fullPath := path.Join(pathPattern, entry.Name)
			if entry.Type == ftp.EntryTypeFile {
				files = append(files, fullPath)
			} else if entry.Type == ftp.EntryTypeFolder && currentLevel < maxLevel {
				subFiles, err := h.getFilesFromPath(fullPath, currentLevel+1, maxLevel)
				if err != nil {
					continue // 跳过无法访问的目录
				}
				files = append(files, subFiles...)
			}
		}
	}

	return files, nil
}

func (h *StandardFtpHelper) expandWildcardPath(pathPattern string, currentLevel, maxLevel int) ([]string, error) {
	// 简单的通配符支持，基于目录结构
	dir := filepath.Dir(pathPattern)
	pattern := filepath.Base(pathPattern)

	entries, err := h.ftpClient.List(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to list directory %s: %v", dir, err)
	}

	var files []string
	for _, entry := range entries {
		// 简单的通配符匹配
		if matched, _ := filepath.Match(pattern, entry.Name); matched {
			fullPath := path.Join(dir, entry.Name)
			if entry.Type == ftp.EntryTypeFile {
				files = append(files, fullPath)
			} else if entry.Type == ftp.EntryTypeFolder && currentLevel < maxLevel {
				subFiles, err := h.getFilesFromPath(fullPath, currentLevel+1, maxLevel)
				if err != nil {
					continue
				}
				files = append(files, subFiles...)
			}
		}
	}

	return files, nil
}

func (h *StandardFtpHelper) GetInputStream(fileName string) (io.ReadCloser, error) {
	response, err := h.ftpClient.Retr(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve file %s: %v", fileName, err)
	}
	return response, nil
}

// SftpHelper 实现SFTP协议
type SftpHelper struct {
	sshClient  *ssh.Client
	sftpClient *sftp.Client
}

func NewSftpHelper() *SftpHelper {
	return &SftpHelper{}
}

func (h *SftpHelper) LoginFtpServer(host, username, password string, port, timeout int, connectPattern string) error {
	address := net.JoinHostPort(host, strconv.Itoa(port))

	config := &ssh.ClientConfig{
		User: username,
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // 注意：生产环境应该验证主机密钥
		Timeout:         time.Duration(timeout) * time.Millisecond,
	}

	// 连接SSH服务器
	sshClient, err := ssh.Dial("tcp", address, config)
	if err != nil {
		return fmt.Errorf("failed to connect to SSH server %s: %v", address, err)
	}

	// 创建SFTP客户端
	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		sshClient.Close()
		return fmt.Errorf("failed to create SFTP client: %v", err)
	}

	h.sshClient = sshClient
	h.sftpClient = sftpClient
	return nil
}

func (h *SftpHelper) LogoutFtpServer() error {
	var err error
	if h.sftpClient != nil {
		if closeErr := h.sftpClient.Close(); closeErr != nil {
			err = closeErr
		}
	}
	if h.sshClient != nil {
		if closeErr := h.sshClient.Close(); closeErr != nil {
			err = closeErr
		}
	}
	return err
}

func (h *SftpHelper) GetAllFiles(paths []string, currentLevel, maxLevel int) ([]string, error) {
	if currentLevel > maxLevel {
		return nil, fmt.Errorf("maximum traversal level exceeded: %d", maxLevel)
	}

	var allFiles []string

	for _, pathPattern := range paths {
		files, err := h.getFilesFromPath(pathPattern, currentLevel, maxLevel)
		if err != nil {
			return nil, fmt.Errorf("failed to get files from path %s: %v", pathPattern, err)
		}
		allFiles = append(allFiles, files...)
	}

	return allFiles, nil
}

func (h *SftpHelper) getFilesFromPath(pathPattern string, currentLevel, maxLevel int) ([]string, error) {
	// 检查路径是否包含通配符
	if strings.Contains(pathPattern, "*") {
		return h.expandWildcardPath(pathPattern, currentLevel, maxLevel)
	}

	// 获取文件信息
	info, err := h.sftpClient.Stat(pathPattern)
	if err != nil {
		return nil, fmt.Errorf("failed to stat path %s: %v", pathPattern, err)
	}

	var files []string
	if info.IsDir() {
		// 目录处理
		entries, err := h.sftpClient.ReadDir(pathPattern)
		if err != nil {
			return nil, fmt.Errorf("failed to read directory %s: %v", pathPattern, err)
		}

		for _, entry := range entries {
			fullPath := path.Join(pathPattern, entry.Name())
			if entry.IsDir() && currentLevel < maxLevel {
				subFiles, err := h.getFilesFromPath(fullPath, currentLevel+1, maxLevel)
				if err != nil {
					continue
				}
				files = append(files, subFiles...)
			} else if !entry.IsDir() {
				files = append(files, fullPath)
			}
		}
	} else {
		// 文件处理
		files = append(files, pathPattern)
	}

	return files, nil
}

func (h *SftpHelper) expandWildcardPath(pathPattern string, currentLevel, maxLevel int) ([]string, error) {
	dir := filepath.Dir(pathPattern)
	pattern := filepath.Base(pathPattern)

	entries, err := h.sftpClient.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %v", dir, err)
	}

	var files []string
	for _, entry := range entries {
		if matched, _ := filepath.Match(pattern, entry.Name()); matched {
			fullPath := path.Join(dir, entry.Name())
			if entry.IsDir() && currentLevel < maxLevel {
				subFiles, err := h.getFilesFromPath(fullPath, currentLevel+1, maxLevel)
				if err != nil {
					continue
				}
				files = append(files, subFiles...)
			} else if !entry.IsDir() {
				files = append(files, fullPath)
			}
		}
	}

	return files, nil
}

func (h *SftpHelper) GetInputStream(fileName string) (io.ReadCloser, error) {
	file, err := h.sftpClient.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %v", fileName, err)
	}
	return file, nil
}