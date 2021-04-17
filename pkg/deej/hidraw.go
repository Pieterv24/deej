package deej

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/omriharel/deej/pkg/deej/util"
	"github.com/sstallion/go-hid"
	"go.uber.org/zap"
)

// HIDRAW provides a deej-aware abstraction to communicate over HID_RAW
type HIDRAW struct {
	vendorId  uint16
	productId uint16
	UsagePage uint16
	Usage     uint16

	deej   *Deej
	logger *zap.SugaredLogger

	stopChannel chan bool
	connected   bool
	hidDevice   *hid.Device

	sliderMoveConsumers []chan SliderMoveEvent
}

func NewHIDRAW(deej *Deej, logger *zap.SugaredLogger) (*HIDRAW, error) {
	logger = logger.Named("hid_raw")

	hidraw := &HIDRAW{
		deej:                deej,
		logger:              logger,
		connected:           false,
		hidDevice:           nil,
		stopChannel:         make(chan bool),
		sliderMoveConsumers: []chan SliderMoveEvent{},
	}

	logger.Debug("Created hid_raw instance")

	hidraw.setupOnConfigReload()

	return hidraw, nil
}

func (hidraw *HIDRAW) Start() error {

	// Init hid library
	hid.Init()

	// don't allow multiple concurrent connections
	if hidraw.connected {
		hidraw.logger.Warn("Already connected, can't start another without closing first")
		return errors.New("serial: connection already active")
	}

	// Get hidraw devices
	var hidDeviceInfo *hid.DeviceInfo
	hid.Enumerate(hidraw.deej.config.HidConnectionInfo.VendorId, hidraw.deej.config.HidConnectionInfo.ProductId,
		func(info *hid.DeviceInfo) error {
			if info.UsagePage == hidraw.deej.config.HidConnectionInfo.UsagePage && info.Usage == hidraw.deej.config.HidConnectionInfo.Usage {
				hidDeviceInfo = info
			}
			return nil
		},
	)

	if hidDeviceInfo == nil {
		hidraw.logger.Warnw("Could not find hidraw device",
			"vendor_id", hidraw.deej.config.HidConnectionInfo.VendorId,
			"product_id", hidraw.deej.config.HidConnectionInfo.ProductId,
			"usage_page", hidraw.deej.config.HidConnectionInfo.UsagePage,
			"usage", hidraw.deej.config.HidConnectionInfo.Usage)
		return errors.New("Could not find hidraw device")
	}

	hidraw.logger.Debugw("Attempting to connect to hidraw device",
		"Device", hidDeviceInfo.ProductStr,
		"Manufacturer", hidDeviceInfo.MfrStr,
		"Path", hidDeviceInfo.Path)

	var err error
	hidraw.hidDevice, err = hid.OpenPath(hidDeviceInfo.Path)
	if err != nil {
		// might need a user notification here, TBD
		hidraw.logger.Warnw("Failed to open HID connection", "error", err)
		return fmt.Errorf("open HID connection: %w", err)
	}

	namedLogger := hidraw.logger.Named(strings.ToLower(
		fmt.Sprintf("%v:%v",
			hidDeviceInfo.MfrStr,
			hidDeviceInfo.ProductStr),
	),
	)

	namedLogger.Info("Connected")
	hidraw.connected = true

	// read hid_raw comms or await a stop
	go func() {
		buffChannel := hidraw.readHID(namedLogger)

		// Send current slider values to controller
		hidraw.sendSliderValues(namedLogger)

		for {
			select {
			case <-hidraw.stopChannel:
				hidraw.close(namedLogger)
			case buff := <-buffChannel:
				hidraw.handleBuff(namedLogger, buff)
			}
		}
	}()

	return nil
}

func (hidraw *HIDRAW) sendSliderValues(logger *zap.SugaredLogger) {
	hidraw.deej.config.SliderMapping.iterate(func(slider int, targets []string) {
		sliderVolume := hidraw.deej.sessions.getSliderVolume(slider, targets)

		sliderVolume *= 100
		percentVolume := uint16(sliderVolume)

		message := make([]byte, 32)
		message[0] = 0x03
		message[1] = 0xFF
		message[2] = byte(slider)
		message[3] = byte((percentVolume >> 8) & 0xFF)
		message[4] = byte(percentVolume & 0xFF)

		logger.Debugf("Writing to device: %v", percentVolume)
		hidraw.hidDevice.Write(message)
	})
}

func (hidraw *HIDRAW) readHID(logger *zap.SugaredLogger) chan []byte {
	ch := make(chan []byte, 32)

	go func() {
		for {
			buff := make([]byte, 32)
			if _, err := hidraw.hidDevice.Read(buff); err != nil {

				if hidraw.deej.Verbose() {
					logger.Warn("Failed to read buffer")
				}

				return
			}

			ch <- buff
		}
	}()

	return ch
}

func (hidraw *HIDRAW) handleBuff(logger *zap.SugaredLogger, buff []byte) {
	// 0xFD signifies a deej command
	if buff[0] == 0xFD {
		if buff[1] == 0xDD {
			logger.Debugf("Got them DD's")
			return
		}
		// The 2nd byte is the adressed slider
		slider := int(buff[1])

		// The 3th and 4th byte contain the value
		val := binary.BigEndian.Uint16(buff[2:4])

		// Doing stuff stolen from serial.go code
		dirtyFloat := float32(val) / 100
		normalizedScalar := util.NormalizeScalar(dirtyFloat)

		// Notify consumers of slider changes
		for _, consumer := range hidraw.sliderMoveConsumers {
			moveEvent := SliderMoveEvent{
				SliderID:     slider,
				PercentValue: normalizedScalar,
			}

			consumer <- moveEvent
		}
	}
}

func (hidraw *HIDRAW) Stop() {
	if hidraw.connected {
		hidraw.logger.Debug("Shutting down hid_raw connection")
		hidraw.stopChannel <- true
	} else {
		hidraw.logger.Debug("Not currently connected")
	}
}

// SubscribeToSliderMoveEvents returns an unbuffered channel that receives
// a sliderMoveEvent struct every time a slider moves
func (hidraw *HIDRAW) SubscribeToSliderMoveEvents() chan SliderMoveEvent {
	ch := make(chan SliderMoveEvent)
	hidraw.sliderMoveConsumers = append(hidraw.sliderMoveConsumers, ch)

	return ch
}

func (hidraw *HIDRAW) close(logger *zap.SugaredLogger) {
	if err := hidraw.hidDevice.Close(); err != nil {
		logger.Warnw("Failed to close hid_raw connection", "error", err)
	} else {
		logger.Debug("hid_raw connection closed")
	}

	hidraw.hidDevice = nil
	hidraw.connected = false
	hid.Exit()
}

func (hidraw *HIDRAW) setupOnConfigReload() {
	configReloadedChannel := hidraw.deej.config.SubscribeToChanges()

	const stopDelay = 50 * time.Millisecond

	go func() {
		for {
			select {
			case <-configReloadedChannel:
				if hidraw.deej.config.HidConnectionInfo.ProductId != hidraw.productId ||
					hidraw.deej.config.HidConnectionInfo.VendorId != hidraw.vendorId ||
					hidraw.deej.config.HidConnectionInfo.UsagePage != hidraw.UsagePage ||
					hidraw.deej.config.HidConnectionInfo.Usage != hidraw.Usage {

					hidraw.logger.Info("Detected change in connection parameters, attempting to renew connection")
					hidraw.Stop()

					// let the connection close
					<-time.After(stopDelay)

					if err := hidraw.Start(); err != nil {
						hidraw.logger.Warnw("Failed to renew connection after parameter change", "error", err)
					} else {
						hidraw.logger.Debug("Renewed connection successfully")
					}
				}
			}
		}
	}()
}
