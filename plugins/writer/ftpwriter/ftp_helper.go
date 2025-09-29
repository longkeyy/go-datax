package ftpwriter

import (
	"fmt"
	"io"
	"net"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/jlaffaye/ftp"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// IFtpHelper 接口定义了FTP和SFTP操作的通用方法 - 与Java版本对应
type IFtpHelper interface {
	LoginFtpServer(host, username, password string, port, timeout int) error
	LogoutFtpServer() error
	MkDirRecursive(dirPath string) error
	GetAllFilesInDir(dirPath, fileName string) (map[string]bool, error)
	DeleteFiles(fileSet map[string]bool) error
	GetOutputStream(fileName string) (io.WriteCloser, error)
}

// StandardFtpHelperImpl 实现标准FTP协议 - 对应Java版本StandardFtpHelperImpl
type StandardFtpHelperImpl struct {
	ftpClient *ftp.ServerConn
}

func NewStandardFtpHelperImpl() *StandardFtpHelperImpl {
	return &StandardFtpHelperImpl{}
}

func (h *StandardFtpHelperImpl) LoginFtpServer(host, username, password string, port, timeout int) error {
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

func (h *StandardFtpHelperImpl) LogoutFtpServer() error {
	if h.ftpClient != nil {
		return h.ftpClient.Quit()
	}
	return nil
}

func (h *StandardFtpHelperImpl) MkDirRecursive(dirPath string) error {
	if dirPath == "" || dirPath == "/" {
		return nil
	}

	// 规范化路径
	dirPath = path.Clean(dirPath)
	if !strings.HasPrefix(dirPath, "/") {
		dirPath = "/" + dirPath
	}

	// 分割路径并递归创建
	parts := strings.Split(strings.Trim(dirPath, "/"), "/")
	currentPath := ""

	for _, part := range parts {
		if part == "" {
			continue
		}
		currentPath = path.Join(currentPath, part)
		if !strings.HasPrefix(currentPath, "/") {
			currentPath = "/" + currentPath
		}

		// 检查目录是否存在
		entries, err := h.ftpClient.List(currentPath)
		if err != nil {
			// 目录不存在，创建它
			if mkdirErr := h.ftpClient.MakeDir(currentPath); mkdirErr != nil {
				return fmt.Errorf("failed to create directory %s: %v", currentPath, mkdirErr)
			}
		} else if len(entries) == 1 && entries[0].Type != ftp.EntryTypeFolder {
			return fmt.Errorf("path %s exists but is not a directory", currentPath)
		}
	}

	return nil
}

func (h *StandardFtpHelperImpl) GetAllFilesInDir(dirPath, fileName string) (map[string]bool, error) {
	fileSet := make(map[string]bool)

	entries, err := h.ftpClient.List(dirPath)
	if err != nil {
		return fileSet, fmt.Errorf("failed to list directory %s: %v", dirPath, err)
	}

	for _, entry := range entries {
		if entry.Type == ftp.EntryTypeFile && strings.HasPrefix(entry.Name, fileName) {
			fileSet[entry.Name] = true
		}
	}

	return fileSet, nil
}

func (h *StandardFtpHelperImpl) DeleteFiles(fileSet map[string]bool) error {
	for fileName := range fileSet {
		if err := h.ftpClient.Delete(fileName); err != nil {
			return fmt.Errorf("failed to delete file %s: %v", fileName, err)
		}
	}
	return nil
}

func (h *StandardFtpHelperImpl) GetOutputStream(fileName string) (io.WriteCloser, error) {
	// 确保目录存在
	dir := path.Dir(fileName)
	if dir != "." && dir != "/" {
		if err := h.MkDirRecursive(dir); err != nil {
			return nil, fmt.Errorf("failed to create directory for file %s: %v", fileName, err)
		}
	}

	// 使用io.Pipe创建写入器
	reader, writer := io.Pipe()

	// 启动goroutine进行异步上传
	go func() {
		defer reader.Close()
		err := h.ftpClient.Stor(fileName, reader)
		if err != nil {
			// 记录错误，关闭reader会导致writer出错
			reader.CloseWithError(err)
		}
	}()

	return writer, nil
}

// SftpHelperImpl 实现SFTP协议 - 对应Java版本SftpHelperImpl
type SftpHelperImpl struct {
	sshClient  *ssh.Client
	sftpClient *sftp.Client
}

func NewSftpHelperImpl() *SftpHelperImpl {
	return &SftpHelperImpl{}
}

func (h *SftpHelperImpl) LoginFtpServer(host, username, password string, port, timeout int) error {
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

func (h *SftpHelperImpl) LogoutFtpServer() error {
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

func (h *SftpHelperImpl) MkDirRecursive(dirPath string) error {
	if dirPath == "" || dirPath == "/" {
		return nil
	}

	// 规范化路径
	dirPath = path.Clean(dirPath)
	if !strings.HasPrefix(dirPath, "/") {
		dirPath = "/" + dirPath
	}

	// 检查目录是否存在
	if _, err := h.sftpClient.Stat(dirPath); err == nil {
		return nil // 目录已存在
	}

	// 递归创建父目录
	parent := path.Dir(dirPath)
	if parent != dirPath && parent != "/" {
		if err := h.MkDirRecursive(parent); err != nil {
			return err
		}
	}

	// 创建当前目录
	if err := h.sftpClient.Mkdir(dirPath); err != nil {
		// 再次检查是否已存在，避免并发创建的竞态条件
		if _, statErr := h.sftpClient.Stat(dirPath); statErr != nil {
			return fmt.Errorf("failed to create directory %s: %v", dirPath, err)
		}
	}

	return nil
}

func (h *SftpHelperImpl) GetAllFilesInDir(dirPath, fileName string) (map[string]bool, error) {
	fileSet := make(map[string]bool)

	entries, err := h.sftpClient.ReadDir(dirPath)
	if err != nil {
		return fileSet, fmt.Errorf("failed to read directory %s: %v", dirPath, err)
	}

	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), fileName) {
			fileSet[entry.Name()] = true
		}
	}

	return fileSet, nil
}

func (h *SftpHelperImpl) DeleteFiles(fileSet map[string]bool) error {
	for fileName := range fileSet {
		if err := h.sftpClient.Remove(fileName); err != nil {
			return fmt.Errorf("failed to delete file %s: %v", fileName, err)
		}
	}
	return nil
}

func (h *SftpHelperImpl) GetOutputStream(fileName string) (io.WriteCloser, error) {
	// 确保目录存在
	dir := path.Dir(fileName)
	if dir != "." && dir != "/" {
		if err := h.MkDirRecursive(dir); err != nil {
			return nil, fmt.Errorf("failed to create directory for file %s: %v", fileName, err)
		}
	}

	// 创建文件
	file, err := h.sftpClient.Create(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to create file %s: %v", fileName, err)
	}

	return file, nil
}