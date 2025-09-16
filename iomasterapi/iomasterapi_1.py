'''Here is the updated, robust, and cleaned-up version of your code,
with the control_base dependency removed and the structure improved for clarity and efficiency.

'''



import sys
import platform
import json
import logging

IS_RPI = False
system_platform = platform.system()

if system_platform == 'Linux':
    try:
        with open('/proc/device-tree/model') as f:
            if 'Raspberry Pi' in f.read():
                IS_RPI = True
    except Exception:
        IS_RPI = False
else:
    IS_RPI = False

if not IS_RPI:
    logging.critical("Not a Raspberry Pi. Exiting.")
    sys.exit(1)

import RPi.GPIO as GPIO
import board
import busio
import adafruit_ads1x15.ads1115 as ADS
from adafruit_ads1x15.analog_in import AnalogIn
from adafruit_pcf8574 import PCF8574
import digitalio

class HardwareController:
    def __init__(self, installer_cfg_path, io_mappings_path):
        self.digital_inputs = {}
        self.digital_outputs = {}
        self.analog_inputs = {}
        
        self.i2c = None
        self.pcf_devices = {}
        self.pcf_pins = {}
        self.ads_devices = {}
        self.ads_channels = {}
        self.gpio_pins_setup = set()

        try:
            GPIO.setmode(GPIO.BCM)
            self._initialize_i2c()
            self._parse_configs(installer_cfg_path, io_mappings_path)
            self._initialize_devices()
            logging.info("HardwareController initialization complete.")
        except Exception as e:
            logging.critical(f"Fatal error during HardwareController initialization: {e}")
            self.cleanup()
            raise

    def __del__(self):
        self.cleanup()

    def cleanup(self):
        try:
            GPIO.cleanup()
            logging.info("GPIO cleanup completed.")
        except RuntimeError:
            pass
        except Exception as e:
            logging.error(f"Error during GPIO cleanup: {e}")

    def _initialize_i2c(self):
        try:
            self.i2c = busio.I2C(board.SCL, board.SDA)
            logging.info("I2C bus initialized successfully.")
        except Exception as e:
            logging.error(f"Failed to initialize I2C bus: {e}")
            self.i2c = None

    @staticmethod
    def _load_json(file_path):
        try:
            with open(file_path, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            logging.error(f"Configuration file not found: {file_path}")
            raise
        except json.JSONDecodeError:
            logging.error(f"Error decoding JSON from file: {file_path}")
            raise

    def _parse_configs(self, installer_cfg_path, io_mappings_path):
        installer_cfg = self._load_json(installer_cfg_path)
        io_mappings = self._load_json(io_mappings_path)

        all_mappings = {
            **io_mappings.get('rpi_pins', {}),
            **io_mappings.get('extender_pins', {}),
            **io_mappings.get('adc_pins', {})
        }

        for device in installer_cfg.get('device_list', []):
            part_num = device.get("part_num")
            if not part_num:
                continue
            
            pinouts = device.get("pinouts", {})
            io_configs = {
                'digital_input': self.digital_inputs,
                'digital_output': self.digital_outputs,
                'analog_input': self.analog_inputs
            }

            for io_type, pin_map in pinouts.items():
                if io_type in io_configs:
                    if part_num not in io_configs[io_type]:
                         io_configs[io_type][part_num] = {}
                    for name, pin_key in pin_map.items():
                        if pin_key in all_mappings:
                            io_configs[io_type][part_num][name] = all_mappings[pin_key]
                        else:
                            logging.warning(f"Pin key '{pin_key}' for {part_num}:{name} not found in io_mappings.")
    
    def _initialize_devices(self):
        self._setup_gpio_pins()
        if self.i2c:
            self._setup_pcf_pins()
            self._setup_ads_pins()
        else:
            logging.warning("I2C not available. Skipping setup for PCF and ADS devices.")

    def _setup_gpio_pins(self):
        for part_num, device in self.digital_inputs.items():
            for name, pin in device.items():
                if isinstance(pin, int) and pin not in self.gpio_pins_setup:
                    try:
                        GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)
                        self.gpio_pins_setup.add(pin)
                        logging.info(f"Set up GPIO input pin {pin} for {part_num}:{name}")
                    except Exception as e:
                        logging.error(f"Failed to set up GPIO input pin {pin}: {e}")

        for part_num, device in self.digital_outputs.items():
            for name, pin in device.items():
                if isinstance(pin, int) and pin not in self.gpio_pins_setup:
                    try:
                        GPIO.setup(pin, GPIO.OUT)
                        self.gpio_pins_setup.add(pin)
                        logging.info(f"Set up GPIO output pin {pin} for {part_num}:{name}")
                    except Exception as e:
                        logging.error(f"Failed to set up GPIO output pin {pin}: {e}")

    def _setup_pcf_pins(self):
        digital_configs = [
            (self.digital_inputs, digitalio.Direction.INPUT, "in"),
            (self.digital_outputs, digitalio.Direction.OUTPUT, "out")
        ]
        for config_dict, direction, key_prefix in digital_configs:
            for part_num, device in config_dict.items():
                for name, info in device.items():
                    if not (isinstance(info, dict) and 'address' in info and 'pin_num' in info):
                        continue
                    
                    address = info['address']
                    pin_num = info['pin_num']
                    
                    if address not in self.pcf_devices:
                        try:
                            self.pcf_devices[address] = PCF8574(self.i2c, int(address, 16))
                            logging.info(f"Initialized PCF8574 at address {address}")
                        except Exception as e:
                            logging.error(f"Failed to initialize PCF8574 at address {address}: {e}")
                            self.pcf_devices[address] = None
                            continue
                    
                    if self.pcf_devices.get(address):
                        try:
                            pin_obj = self.pcf_devices[address].get_pin(pin_num)
                            pin_obj.direction = direction
                            pin_key = f"{key_prefix}_{address}_{pin_num}"
                            self.pcf_pins[pin_key] = pin_obj
                            logging.info(f"Set up PCF pin {pin_num} at {address} for {part_num}:{name}")
                        except Exception as e:
                            logging.error(f"Failed to set up PCF pin {pin_num} at {address}: {e}")

    def _setup_ads_pins(self):
        for part_num, device in self.analog_inputs.items():
            for name, info in device.items():
                if not (isinstance(info, dict) and 'address' in info and 'pin_num' in info):
                    continue
                
                address = info['address']
                channel_name = info['pin_num']

                if address not in self.ads_devices:
                    try:
                        self.ads_devices[address] = ADS.ADS1115(self.i2c, address=int(address, 16))
                        logging.info(f"Initialized ADS1115 at address {address}")
                    except Exception as e:
                        logging.error(f"Failed to initialize ADS1115 at address {address}: {e}")
                        self.ads_devices[address] = None
                        continue
                
                if self.ads_devices.get(address):
                    try:
                        channel_enum = getattr(ADS, channel_name)
                        channel_obj = AnalogIn(self.ads_devices[address], channel_enum)
                        channel_key = f"{address}_{channel_name}"
                        self.ads_channels[channel_key] = channel_obj
                        logging.info(f"Set up ADS channel {channel_name} at {address} for {part_num}:{name}")
                    except Exception as e:
                        logging.error(f"Failed to set up ADS channel {channel_name} at {address}: {e}")

    def read_all_inputs(self):
        return {
            "digital": self.read_digital_inputs(),
            "analog": self.read_analog_inputs()
        }

    def read_digital_inputs(self):
        data = {}
        for part_num, device in self.digital_inputs.items():
            data[part_num] = {}
            for name, info in device.items():
                state = None
                try:
                    if isinstance(info, int):
                        state = not GPIO.input(info)
                    elif isinstance(info, dict):
                        key = f"in_{info['address']}_{info['pin_num']}"
                        if key in self.pcf_pins:
                            state = not self.pcf_pins[key].value
                        else:
                            logging.warning(f"PCF pin for {part_num}:{name} not available.")
                except Exception as e:
                    logging.error(f"Error reading digital input {part_num}:{name}: {e}")
                data[part_num][name] = state
        return data

    def read_analog_inputs(self):
        data = {}
        for part_num, device in self.analog_inputs.items():
            data[part_num] = {}
            for name, info in device.items():
                value = None
                try:
                    if isinstance(info, dict):
                        key = f"{info['address']}_{info['pin_num']}"
                        if key in self.ads_channels:
                            voltage = self.ads_channels[key].voltage
                            value = voltage
                            if 'mapping' in info and all(k in info['mapping'] for k in ['v_min', 'v_max', 'out_min', 'out_max']):
                                m = info['mapping']
                                value = ((voltage - m['v_min']) / (m['v_max'] - m['v_min'])) * (m['out_max'] - m['out_min']) + m['out_min']
                        else:
                            logging.warning(f"ADS channel for {part_num}:{name} not available.")
                except Exception as e:
                    logging.error(f"Error reading analog input {part_num}:{name}: {e}")
                data[part_num][name] = value
        return data

    def write_digital_outputs(self, output_data):
        results = {}
        for part_num, states in output_data.items():
            results[part_num] = {}
            for pin_name, state in states.items():
                results[part_num][pin_name] = self.write_digital_output(part_num, pin_name, state)
        return results

    def write_digital_output(self, part_num, pin_name, state):
        if part_num not in self.digital_outputs or pin_name not in self.digital_outputs[part_num]:
            logging.error(f"Output {part_num}:{pin_name} not found in configuration.")
            return False

        pin_info = self.digital_outputs[part_num][pin_name]
        try:
            if isinstance(pin_info, int):
                GPIO.output(pin_info, int(state))
                return True
            elif isinstance(pin_info, dict):
                key = f"out_{pin_info['address']}_{pin_info['pin_num']}"
                if key in self.pcf_pins:
                    self.pcf_pins[key].value = bool(state)
                    return True
                else:
                    logging.error(f"PCF pin for {part_num}:{pin_name} not available for writing.")
                    return False
            else:
                logging.error(f"Unknown pin format for {part_num}:{pin_name}.")
                return False
        except Exception as e:
            logging.error(f"Error writing to {part_num}:{pin_name}: {e}")
            return False