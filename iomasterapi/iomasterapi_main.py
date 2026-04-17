import sys
import platform

IS_RPI = False
system_platform = platform.system()

if system_platform == 'Linux':
    try:
        with open('/proc/device-tree/model') as f:
            model = f.read()
            if 'Raspberry Pi' in model:
                IS_RPI = True
    except Exception:
        IS_RPI = False
elif system_platform == 'Windows':
    IS_RPI = False
else:
    IS_RPI = False

if not IS_RPI:
    sys.exit(1)

import sys
import os
from datetime import datetime
import json
import time
import random
import enum
import logging
from typing import Union

import RPi.GPIO as GPIO

import board
import busio
import adafruit_ads1x15.ads1115 as ADS
from adafruit_ads1x15.analog_in import AnalogIn

import adafruit_mcp230xx
import digitalio
from adafruit_pcf8574 import PCF8574
from adafruit_mcp230xx.mcp23017 import MCP23017

base_path = "../"
sys.path.insert(0, base_path)
sys.path.insert(0, base_path + 'control/')
from control import control_base as ctrl

GPIO.setmode(GPIO.BCM)

#class ioDevice inherits from ctrl.systemDevice in control_base
class ioDevice(ctrl.systemDevice):
    def __init__(self, devicetype, commtype):
        self.installer_cfg = self.load_json(base_path + 'installer_cfg/installer_cfg.json')   #load installer_cfg for io device configuration
        self.io_mappings = self.load_json(base_path + 'io_mappings/io_mappings.json')    #load io_mappings for pin configuration

        self.i2c = busio.I2C(board.SCL, board.SDA)

        self.io_devices = {}
        self.digital_inputs = {}
        self.digital_outputs = {}
        self.analog_inputs = {}

        self.previous_states = {}

        self.pcf_devices = {}
        self.pcf_pins = {}
        self.gpio_pins_setup = set()
        self.ads_devices = {}
        self.ads_channels = {}


        self.get_io_devices()
        
        self.setup_gpio_pins()
        self.setup_pcf_pins()
        self.setup_ads_pins()

        super().__init__(devicetype, commtype)

        logging.info("ioDevice initialization complete")

    def __del__(self):
        try:
            GPIO.cleanup()
            logging.info("GPIO cleanup completed")
        except Exception as e:
            logging.error(f"GPIO cleanup error: {e}")

    @staticmethod
    def load_json(file_path):
        with open(file_path, 'r') as file:
            return json.load(file)

    def get_io_devices(self):
        if 'device_list' not in self.installer_cfg:
            logging.error("No device_list found in installer configuration")
            return
        
        device_list = self.installer_cfg['device_list']
        for device in device_list:
            if device['comm_type'] == 'gpio':
                part_num = device["part_num"]
                self.io_devices[part_num] = {"digital_input": {}, "digital_output": {}, "analog_input": {}}

                for type, mappings in device["pinouts"].items():
                    for pin_name, pin in mappings.items():
                        # Replace pin values using io_mappings.json
                        if pin in self.io_mappings['rpi_pins']:
                            self.io_devices[part_num][type][pin_name] = self.io_mappings['rpi_pins'][pin]
                        elif pin in self.io_mappings['adc_pins']:
                            self.io_devices[part_num][type][pin_name] = self.io_mappings['adc_pins'][pin]
                        elif pin in self.io_mappings['extender_pins']:
                            self.io_devices[part_num][type][pin_name] = self.io_mappings['extender_pins'][pin]
                        else:
                            logging.warning(f"Pin {pin} not found in io_mappings.json")

        self.separate_io_devices()

    def separate_io_devices(self):
        for part_num, device in self.io_devices.items():
            if device["analog_input"]:
                self.analog_inputs[part_num] = {"analog_input": device["analog_input"]}

            if device["digital_input"]:
                self.digital_inputs[part_num] = {"digital_input": device["digital_input"]}
            
            if device["digital_output"]:
                self.digital_outputs[part_num] = {"digital_output": device["digital_output"]}

    def get_digital_inputs(self):
        return self.digital_inputs

    def get_digital_outputs(self):
        return self.digital_outputs

    def get_analog_inputs(self):
        return self.analog_inputs

    def setup_gpio_pins(self):
        digital_inputs = self.get_digital_inputs()
        for part_num, device_info in digital_inputs.items():
            for pin_name, pin_info in device_info['digital_input'].items():
                if isinstance(pin_info, int):   #setup only for direct gpio pins
                    try:
                        if pin_info not in self.gpio_pins_setup:
                            GPIO.setup(pin_info, GPIO.IN, pull_up_down=GPIO.PUD_UP)
                            self.gpio_pins_setup.add(pin_info)
                            logging.info(f"Set up GPIO input pin {pin_info} for {part_num}:{pin_name}")
                    except Exception as e:
                        logging.error(f"Failed to set up GPIO input pin {pin_info} for {part_num}:{pin_name}: {e}")
        
        digital_outputs = self.get_digital_outputs()
        for part_num, device_info in digital_outputs.items():
            for pin_name, pin_info in device_info['digital_output'].items():
                if isinstance(pin_info, int):
                    try:
                        if pin_info not in self.gpio_pins_setup:
                            GPIO.setup(pin_info, GPIO.OUT)
                            self.gpio_pins_setup.add(pin_info)
                            logging.info(f"Set up GPIO output pin {pin_info} for {part_num}:{pin_name}")
                    except Exception as e:
                        logging.error(f"Failed to set up GPIO output pin {pin_info} for {part_num}:{pin_name}: {e}")
    

    def setup_pcf_pins(self):
        all_pins = {
            "input": self.get_digital_inputs(),
            "output": self.get_digital_outputs()
        }

        pcf_addresses = set()
        for io_type, devices in all_pins.items():
            for part_num, device_info in devices.items():
                io_key = 'digital_input' if io_type == 'input' else 'digital_output'
                for pin_name, pin_info in device_info[io_key].items():
                    if isinstance(pin_info, dict) and 'address' in pin_info:
                        pcf_addresses.add(pin_info['address'])    #only unique addresses will added
        
        try:
            i2c = busio.I2C(board.SCL, board.SDA)
            logging.info("I2C bus initialized")
        except Exception as e:
            logging.error(f"Failed to initialize I2C bus: {e}")
            return
        

        for address in pcf_addresses:
            try:
                addr_int = int(address, 16)
                pcf = PCF8574(i2c, addr_int)
                self.pcf_devices[address] = pcf
                logging.info(f"Initialized PCF8574 at address {address}")
            except Exception as e:
                logging.error(f"Failed to initialize PCF8574 at address {address}: {e}")
        
        digital_inputs = self.get_digital_inputs()
        for part_num, device_info in digital_inputs.items():
            for pin_name, pin_info in device_info['digital_input'].items():
                if isinstance(pin_info, dict) and 'address' in pin_info and 'pin_num' in pin_info:
                    address = pin_info['address']
                    pin_num = pin_info['pin_num']
                    
                    if address in self.pcf_devices and 0 <= pin_num <= 7:
                        try:
                            self.pcf_devices[address].get_pin(pin_num).direction = digitalio.Direction.INPUT
                            
                            pin_key = f"in_{address}_{pin_num}"
                            self.pcf_pins[pin_key] = self.pcf_devices[address].get_pin(pin_num)
                            logging.info(f"Set up PCF8574 input pin {pin_num} at address {address} for {part_num}:{pin_name}")
                        except Exception as e:
                            logging.error(f"Failed to set up PCF8574 input pin {pin_num} at address {address}: {e}")

        digital_outputs = self.get_digital_outputs()
        for part_num, device_info in digital_outputs.items():
            for pin_name, pin_info in device_info['digital_output'].items():
                if isinstance(pin_info, dict) and 'address' in pin_info and 'pin_num' in pin_info:
                    address = pin_info['address']
                    pin_num = pin_info['pin_num']
                    
                    if address in self.pcf_devices and 0 <= pin_num <= 7:
                        try:
                            self.pcf_devices[address].get_pin(pin_num).direction = digitalio.Direction.OUTPUT
                            
                            pin_key = f"out_{address}_{pin_num}"
                            self.pcf_pins[pin_key] = self.pcf_devices[address].get_pin(pin_num)
                            logging.info(f"Set up PCF8574 output pin {pin_num} at address {address} for {part_num}:{pin_name}")
                        except Exception as e:
                            logging.error(f"Failed to set up PCF8574 output pin {pin_num} at address {address}: {e}")

    
    def setup_ads_pins(self):
        analog_inputs = self.get_analog_inputs()

        ads_addresses = set()
        for part_num, device_info in analog_inputs.items():
            for pin_name, pin_info in device_info['analog_input'].items():
                if isinstance(pin_info, dict) and 'address' in pin_info:
                    ads_addresses.add(pin_info['address'])

        try:
            if not hasattr(self, 'i2c') or self.i2c is None:
                i2c = busio.I2C(board.SCL, board.SDA)
                self.i2c = i2c
                logging.info("I2C bus initialized for ADS devices")
        except Exception as e:
            logging.error(f"Failed to initialize I2C bus for ADS devices: {e}")
            return
        
        for address in ads_addresses:
            try:
                addr_int = int(address, 16)
                ads = ADS.ADS1115(self.i2c, address=addr_int)
                self.ads_devices[address] = ads
                logging.info(f"Initialized ADS1115 at address {address}")
            except Exception as e:
                logging.error(f"Failed to initialize ADS1115 at address {address}: {e}")

        for part_num, device_info in analog_inputs.items():
            for pin_name, pin_info in device_info['analog_input'].items():
                if isinstance(pin_info, dict) and 'address' in pin_info and 'pin_num' in pin_info:
                    address = pin_info['address']
                    channel = pin_info['pin_num']  #pin_num is P0, P1 in string format
                    chan_enum = getattr(ADS, channel)
                    
                    if address in self.ads_devices:
                        try:
                            chan = AnalogIn(self.ads_devices[address], chan_enum)    #AnalogIn(ads1, ADS.P0) Problem is ADS.P0 how to initialize them because it is in string format
                            
                            channel_key = f"{address}_{channel}"
                            self.ads_channels[channel_key] = chan
                            logging.info(f"Set up ADS1115 channel {channel} at address {address} for {part_num}:{pin_name}")
                        except Exception as e:
                            logging.error(f"Failed to set up ADS1115 channel {channel} at address {address}: {e}")

    

    def read_digital_inputs(self):
        digital_inputs = self.get_digital_inputs()
        data = {}

        for part_num, device_info in digital_inputs.items():
            data[part_num] = {}
            
            for pin_name, pin_info in device_info['digital_input'].items():
                try:
                    if isinstance(pin_info, int):
                        state = not GPIO.input(pin_info)
                        data[part_num][pin_name] = state
                        logging.debug(f"Read GPIO input {part_num}:{pin_name} on pin {pin_info}: {state}")
                    
                    elif isinstance(pin_info, dict) and 'address' in pin_info and 'pin_num' in pin_info:
                        address = pin_info['address']
                        pin_num = pin_info['pin_num']
                        pin_key = f"in_{address}_{pin_num}"
                        
                        if pin_key in self.pcf_pins:
                            state = not self.pcf_pins[pin_key].value
                            data[part_num][pin_name] = state
                            logging.debug(f"Read PCF8574 input {part_num}:{pin_name} on address {address}, pin {pin_num}: {state}")
                        else:
                            logging.warning(f"PCF8574 pin object for {address}:{pin_num} not found, skipping {part_num}:{pin_name}")
                            data[part_num][pin_name] = None
                    
                    else:
                        logging.warning(f"Unknown pin format for {part_num}:{pin_name}: {pin_info}")
                        data[part_num][pin_name] = None
                        
                except Exception as e:
                    logging.error(f"Error reading input {part_num}:{pin_name}: {e}")
                    data[part_num][pin_name] = None

        return data
    
    def read_analog_inputs(self):
        analog_inputs = self.get_analog_inputs()
        data = {}

        for part_num, device_info in analog_inputs.items():
            data[part_num] = {}
            
            for pin_name, pin_info in device_info['analog_input'].items():
                try:
                    if isinstance(pin_info, dict) and 'address' in pin_info and 'pin_num' in pin_info:
                        address = pin_info['address']
                        channel = pin_info['pin_num']
                        channel_key = f"{address}_{channel}"
                        
                        if channel_key in self.ads_channels:
                            chan = self.ads_channels[channel_key]
                            voltage = chan.voltage
                            value = chan.value
                            
                            v_min = 0
                            v_max = 5
                            t_min = 0
                            t_max = 120

                            Temp = ((voltage - v_min) / (v_max - v_min)) * (t_max - t_min) + t_min

                            
                            data[part_num][pin_name] = Temp
                            
                            logging.debug(f"Read ADS1115 channel {part_num}:{pin_name} on address {address}, channel {channel}: {voltage}V, {value}")
                        else:
                            logging.warning(f"ADS1115 channel for {address}:{channel} not found, skipping {part_num}:{pin_name}")
                            data[part_num][pin_name] = None
                    
                    else:
                        logging.warning(f"Unknown pin format for analog input {part_num}:{pin_name}: {pin_info}")
                        data[part_num][pin_name] = None
                        
                except Exception as e:
                    logging.error(f"Error reading analog input {part_num}:{pin_name}: {e}")
                    data[part_num][pin_name] = None
        
        return data
    


    def write_digital_outputs(self, output_data):
        results = {}
        
        for part_num, states in output_data.items():
            results[part_num] = {}
            
            for pin_name, state in states.items():
                results[part_num][pin_name] = self.write_digital_output(part_num, pin_name, state)
        
        return results
    
    def write_digital_output(self, part_num, pin_name, state):
        digital_outputs = self.get_digital_outputs()

        if part_num not in digital_outputs:
            logging.error(f"Part number {part_num} not found in digital outputs")
            return False
            
        if pin_name not in digital_outputs[part_num]['digital_output']:
            logging.error(f"Pin name {pin_name} not found in part {part_num} digital outputs")
            return False

        pin_info = digital_outputs[part_num]['digital_output'][pin_name]

        try:
            if isinstance(pin_info, int):
                if state == 1:
                    GPIO.output(pin_info, GPIO.state)
                elif state == 0:
                    GPIO.output(pin_info, GPIO.LOW)
                logging.info(f"Set GPIO output {part_num}:{pin_name} on pin {pin_info} to {state}")
                return True
            
            elif isinstance(pin_info, dict) and 'address' in pin_info and 'pin_num' in pin_info:
                address = pin_info['address']
                pin_num = pin_info['pin_num']
                pin_key = f"out_{address}_{pin_num}"
                
                if pin_key in self.pcf_pins:
                    self.pcf_pins[pin_key].value = state
                    logging.info(f"Set PCF8574 output {part_num}:{pin_name} on address {address}, pin {pin_num} to {state}")
                    return True
                else:
                    logging.error(f"PCF8574 pin object for {address}:{pin_num} not found, cannot set {part_num}:{pin_name}")
                    return False
            
            else:
                logging.error(f"Unknown pin format for {part_num}:{pin_name}: {pin_info}")
                return False
                
        except Exception as e:
            logging.error(f"Error setting output {part_num}:{pin_name} to {state}: {e}")
            return False
        

    def decodeIOData(self):
        self.measured_data.digital = self.read_digital_inputs()
        self.measured_data.analog = self.read_analog_inputs()
        

