package crypto

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/backing-image-manager/pkg/types"
	lhns "github.com/longhorn/go-common-libs/ns"
	lhtypes "github.com/longhorn/go-common-libs/types"
)

const (
	MapperFilePathPrefix = "/dev/mapper"

	CryptoKeyDefaultCipher = "aes-xts-plain64"
	CryptoKeyDefaultHash   = "sha256"
	CryptoKeyDefaultSize   = "256"
	CryptoDefaultPBKDF     = "argon2i"
)

// EncryptParams keeps the customized cipher options from the secret CR
type EncryptParams struct {
	KeyProvider string
	KeyCipher   string
	KeyHash     string
	KeySize     string
	PBKDF       string
}

func NewEncryptParams(keyProvider, keyCipher, keyHash, keySize, pbkdf string) *EncryptParams {
	return &EncryptParams{KeyProvider: keyProvider, KeyCipher: keyCipher, KeyHash: keyHash, KeySize: keySize, PBKDF: pbkdf}
}

func (cp *EncryptParams) GetKeyCipher() string {
	if cp.KeyCipher == "" {
		return CryptoKeyDefaultCipher
	}
	return cp.KeyCipher
}

func (cp *EncryptParams) GetKeyHash() string {
	if cp.KeyHash == "" {
		return CryptoKeyDefaultHash
	}
	return cp.KeyHash
}

func (cp *EncryptParams) GetKeySize() string {
	if cp.KeySize == "" {
		return CryptoKeyDefaultSize
	}
	return cp.KeySize
}

func (cp *EncryptParams) GetPBKDF() string {
	if cp.PBKDF == "" {
		return CryptoDefaultPBKDF
	}
	return cp.PBKDF
}

// EncryptBackingImage encrypts provided device with LUKS.
func EncryptBackingImage(devicePath, passphrase string, cryptoParams *EncryptParams) error {
	namespaces := []lhtypes.Namespace{lhtypes.NamespaceMnt, lhtypes.NamespaceIpc}
	nsexec, err := lhns.NewNamespaceExecutor(lhtypes.ProcessNone, lhtypes.HostProcDirectory, namespaces)
	if err != nil {
		return err
	}

	logrus.Infof("Encrypting device %s with LUKS", devicePath)
	if _, err := nsexec.LuksFormat(
		devicePath, passphrase,
		cryptoParams.GetKeyCipher(),
		cryptoParams.GetKeyHash(),
		cryptoParams.GetKeySize(),
		cryptoParams.GetPBKDF(),
		lhtypes.LuksTimeout); err != nil {
		return errors.Wrapf(err, "failed to encrypt device %s with LUKS", devicePath)
	}
	return nil
}

// OpenBackingImage opens backing image so that it can be used by the client.
func OpenBackingImage(devicePath, passphrase, uuid string) error {
	if isOpen, _ := IsEncryptedDeviceOpened(types.BackingImageMapper(uuid)); isOpen {
		logrus.Infof("Device %s is already opened at %s", devicePath, types.BackingImageMapper(uuid))
		return nil
	}

	namespaces := []lhtypes.Namespace{lhtypes.NamespaceMnt, lhtypes.NamespaceIpc}
	nsexec, err := lhns.NewNamespaceExecutor(lhtypes.ProcessNone, lhtypes.HostProcDirectory, namespaces)
	if err != nil {
		return err
	}

	logrus.Infof("Opening device %s with LUKS on %v", devicePath, types.BackingImageFileName)
	_, err = nsexec.LuksOpen(types.GetLuksBackingImageName(uuid), devicePath, passphrase, lhtypes.LuksTimeout)
	if err != nil {
		logrus.WithError(err).Warnf("Failed to open LUKS device %s", devicePath)
	}
	return err
}

// CloseBackingImage closes encrypted backing image so it can be detached.
func CloseBackingImage(uuid string) error {
	namespaces := []lhtypes.Namespace{lhtypes.NamespaceMnt, lhtypes.NamespaceIpc}
	nsexec, err := lhns.NewNamespaceExecutor(lhtypes.ProcessNone, lhtypes.HostProcDirectory, namespaces)
	if err != nil {
		return err
	}

	logrus.Infof("Closing LUKS device %s", types.GetLuksBackingImageName(uuid))
	_, err = nsexec.LuksClose(types.GetLuksBackingImageName(uuid), lhtypes.LuksTimeout)
	return err
}

// IsEncryptedDeviceOpened determines if encrypted device is already open.
func IsEncryptedDeviceOpened(device string) (bool, error) {
	_, mappedFile, err := DeviceEncryptionStatus(device)
	return mappedFile != "", err
}

// DeviceEncryptionStatus looks to identify if the passed device is a LUKS mapping
// and if so what the device is and the mapper name as used by LUKS.
// If not, just returns the original device and an empty string.
func DeviceEncryptionStatus(devicePath string) (mappedDevice, mapper string, err error) {
	if !strings.HasPrefix(devicePath, types.MapperFilePathPrefix) {
		return devicePath, "", nil
	}

	namespaces := []lhtypes.Namespace{lhtypes.NamespaceMnt, lhtypes.NamespaceIpc}
	nsexec, err := lhns.NewNamespaceExecutor(lhtypes.ProcessNone, lhtypes.HostProcDirectory, namespaces)
	if err != nil {
		return devicePath, "", err
	}

	backingImage := strings.TrimPrefix(devicePath, types.MapperFilePathPrefix+"/")
	stdout, err := nsexec.LuksStatus(backingImage, lhtypes.LuksTimeout)
	if err != nil {
		logrus.WithError(err).Warnf("Device %s is not an active LUKS device", devicePath)
		return devicePath, "", nil
	}

	lines := strings.Split(string(stdout), "\n")
	if len(lines) < 1 {
		return "", "", fmt.Errorf("device encryption status returned no stdout for %s", devicePath)
	}

	if !strings.Contains(lines[0], " is active") {
		// Implies this is not a LUKS device
		return devicePath, "", nil
	}

	for i := 1; i < len(lines); i++ {
		kv := strings.SplitN(strings.TrimSpace(lines[i]), ":", 2)
		if len(kv) < 1 {
			return "", "", fmt.Errorf("device encryption status output for %s is badly formatted: %s",
				devicePath, lines[i])
		}
		if strings.Compare(kv[0], "device") == 0 {
			return strings.TrimSpace(kv[1]), backingImage, nil
		}
	}
	// Identified as LUKS, but failed to identify a mapped device
	return "", "", fmt.Errorf("mapped device not found in path %s", devicePath)
}
