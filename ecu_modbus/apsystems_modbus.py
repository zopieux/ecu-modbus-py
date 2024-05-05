import enum
import time

from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadBuilder
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.client import ModbusTcpClient
from pymodbus.client import ModbusSerialClient
from pymodbus.register_read_message import ReadHoldingRegistersResponse


class SunspecDID(enum.Enum):
    SINGLE_PHASE_INVERTER = 101
    SPLIT_PHASE_INVERTER = 102
    THREE_PHASE_INVERTER = 103
    SINGLE_PHASE_METER = 201
    SPLIT_PHASE_METER = 202
    WYE_THREE_PHASE_METER = 203
    DELTA_THREE_PHASE_METER = 204


class InverterStatus(enum.Enum):
    I_STATUS_OFF = 1
    I_STATUS_SLEEPING = 2
    I_STATUS_STARTING = 3
    I_STATUS_MPPT = 4
    I_STATUS_THROTTLED = 5
    I_STATUS_SHUTTING_DOWN = 6
    I_STATUS_FAULT = 7
    I_STATUS_STANDBY = 8


class ConnectionType(enum.Enum):
    RTU = 1
    TCP = 2


class RegisterType(enum.Enum):
    INPUT = 1
    HOLDING = 2


class RegisterDataType(enum.Enum):
    UINT16 = 1
    UINT32 = 2
    UINT64 = 3
    INT16 = 4
    SCALE = 4
    ACC32 = 5
    FLOAT32 = 6
    SEFLOAT = 7
    STRING = 9


SUNSPEC_NOT_IMPLEMENTED = {
    "UINT16": 0xFFFF,
    "UINT32": 0xFFFFFFFF,
    "UINT64": 0xFFFFFFFFFFFFFFFF,
    "INT16": 0x8000,
    "SCALE": 0x8000,
    "ACC32": 0x00000000,
    "FLOAT32": 0x7FC00000,
    "SEFLOAT": 0xFFFFFFFF,
    "STRING": "",
}

C_SUNSPEC_DID_MAP = {
    "101": "Single Phase Inverter",
    "102": "Split Phase Inverter",
    "103": "Three Phase Inverter",
    "201": "Single Phase Meter",
    "202": "Split Phase Meter",
    "203": "Wye 3P1N Three Phase Meter",
    "204": "Delta 3P Three Phase Meter",
}

INVERTER_STATUS_MAP = [
    "Undefined",
    "Off",
    "Sleeping",
    "Grid Monitoring",
    "Producing",
    "Producing (Throttled)",
    "Shutting Down",
    "Fault",
    "Standby",
]


METER_REGISTER_OFFSETS = [
    0x000,
    0x0AE,
    0x15C,
]


class APsystems:

    model = "APsystems"
    stopbits = 1
    parity = "N"
    baud = 115200
    wordorder = Endian.BIG

    def __init__(
        self,
        host=None,
        port=None,
        device=None,
        unit=1,
        stopbits=None,
        parity=None,
        baud=None,
        timeout=2.0,
        retries=3,
        parent=None,
    ):
        if parent:
            self.client = parent.client
            self.mode = parent.mode
            self.timeout = parent.timeout
            self.retries = parent.retries
            self.unit = parent.unit

            if self.mode is ConnectionType.RTU:
                self.device = parent.device
                self.stopbits = parent.stopbits
                self.parity = parent.parity
                self.baud = parent.baud
            elif self.mode is ConnectionType.TCP:
                self.host = parent.host
                self.port = parent.port
            else:
                raise NotImplementedError(self.mode)
        else:
            self.host = host
            self.port = port
            self.device = device

            if stopbits is not None:
                self.stopbits = stopbits

            if parity is not None and parity.upper() in ["N", "E", "O"]:
                self.parity = parity.upper()

            if baud is not None:
                self.baud = baud

            self.timeout = timeout
            self.retries = retries
            self.unit = unit

            if device is not None:
                self.mode = ConnectionType.RTU
                self.client = ModbusSerialClient(
                    method="rtu",
                    port=self.device,
                    stopbits=self.stopbits,
                    parity=self.parity,
                    baudrate=self.baud,
                    timeout=self.timeout,
                )
            else:
                self.mode = ConnectionType.TCP
                self.client = ModbusTcpClient(
                    host=self.host, port=self.port, timeout=self.timeout
                )

    def __repr__(self):
        if self.mode == ConnectionType.RTU:
            return f"{self.model}({self.device}, {self.mode}: stopbits={self.stopbits}, parity={self.parity}, baud={self.baud}, timeout={self.timeout}, retries={self.retries}, unit={hex(self.unit)})"
        elif self.mode == ConnectionType.TCP:
            return f"{self.model}({self.host}:{self.port}, {self.mode}: timeout={self.timeout}, retries={self.retries}, unit={hex(self.unit)})"
        else:
            return f"<{self.__class__.__module__}.{self.__class__.__name__} object at {hex(id(self))}>"

    def _read_holding_registers(self, address, length):
        for i in range(self.retries):
            if not self.connected():
                self.connect()
                time.sleep(0.1)
                continue

            result = self.client.read_holding_registers(address, length, slave=self.unit)

            if not isinstance(result, ReadHoldingRegistersResponse):
                continue
            if len(result.registers) != length:
                continue

            return BinaryPayloadDecoder.fromRegisters(
                result.registers, byteorder=Endian.BIG, wordorder=self.wordorder
            )

        return None

    def _write_holding_register(self, address, value):
        return self.client.write_registers(
            address=address, values=value, unit=self.unit
        )

    def _encode_value(self, data, dtype):
        builder = BinaryPayloadBuilder(byteorder=Endian.BIG, wordorder=self.wordorder)

        try:
            if dtype == RegisterDataType.UINT16:
                builder.add_16bit_uint(data)
            elif dtype == RegisterDataType.UINT32:
                builder.add_32bit_uint(data)
            elif dtype == RegisterDataType.UINT64:
                builder.add_64bit_uint(data)
            elif dtype == RegisterDataType.INT16:
                builder.add_16bit_int(data)
            elif dtype == RegisterDataType.FLOAT32 or dtype == RegisterDataType.SEFLOAT:
                builder.add_32bit_float(data)
            elif dtype == RegisterDataType.STRING:
                builder.add_string(data)
            else:
                raise NotImplementedError(dtype)

        except NotImplementedError:
            raise

        return builder.to_registers()

    def _decode_value(self, data, length, dtype, vtype):
        try:
            if dtype == RegisterDataType.UINT16:
                decoded = data.decode_16bit_uint()
            elif dtype == RegisterDataType.UINT32 or dtype == RegisterDataType.ACC32:
                decoded = data.decode_32bit_uint()
            elif dtype == RegisterDataType.UINT64:
                decoded = data.decode_64bit_uint()
            elif dtype == RegisterDataType.INT16:
                decoded = data.decode_16bit_int()
            elif dtype == RegisterDataType.FLOAT32 or dtype == RegisterDataType.SEFLOAT:
                decoded = data.decode_32bit_float()
            elif dtype == RegisterDataType.STRING:
                decoded = (
                    data.decode_string(length * 2)
                    .decode(encoding="utf-8", errors="ignore")
                    .replace("\x00", "")
                    .rstrip()
                )
            else:
                raise NotImplementedError(dtype)

            if decoded == SUNSPEC_NOT_IMPLEMENTED[dtype.name]:
                return vtype(False)
            else:
                return vtype(decoded)
        except NotImplementedError:
            raise

    def _read(self, value):
        address, length, rtype, dtype, vtype, label, fmt, batch = value

        try:
            if rtype == RegisterType.INPUT:
                return self._decode_value(
                    self._read_input_registers(address, length), length, dtype, vtype
                )
            elif rtype == RegisterType.HOLDING:
                return self._decode_value(
                    self._read_holding_registers(address, length), length, dtype, vtype
                )
            else:
                raise NotImplementedError(rtype)
        except NotImplementedError:
            raise
        except AttributeError:
            return False

    def _read_all(self, values, rtype):
        addr_min = False
        addr_max = False

        for k, v in values.items():
            v_addr = v[0]
            v_length = v[1]

            if addr_min is False:
                addr_min = v_addr
            if addr_max is False:
                addr_max = v_addr + v_length

            if v_addr < addr_min:
                addr_min = v_addr
            if (v_addr + v_length) > addr_max:
                addr_max = v_addr + v_length

        results = {}
        offset = addr_min
        length = addr_max - addr_min

        try:
            if rtype == RegisterType.INPUT:
                data = self._read_input_registers(offset, length)
            elif rtype == RegisterType.HOLDING:
                data = self._read_holding_registers(offset, length)
            else:
                raise NotImplementedError(rtype)

            if not data:
                return results

            for k, v in values.items():
                address, length, rtype, dtype, vtype, label, fmt, batch = v

                if address > offset:
                    skip_bytes = address - offset
                    offset += skip_bytes
                    data.skip_bytes(skip_bytes * 2)

                results[k] = self._decode_value(data, length, dtype, vtype)
                offset += length
        except NotImplementedError:
            raise

        return results

    def _write(self, value, data):
        address, length, rtype, dtype, vtype, label, fmt, batch = value

        try:
            if rtype == RegisterType.HOLDING:
                return self._write_holding_register(
                    address, self._encode_value(data, dtype)
                )
            else:
                raise NotImplementedError(rtype)
        except NotImplementedError:
            raise

    def connect(self):
        return self.client.connect()

    def disconnect(self):
        self.client.close()

    def connected(self):
        return self.client.is_socket_open()

    def read(self, key):
        if key not in self.registers:
            raise KeyError(key)

        return {key: self._read(self.registers[key])}

    def write(self, key, data):
        if key not in self.registers:
            raise KeyError(key)

        return self._write(self.registers[key], data)

    def read_all(self, rtype=RegisterType.HOLDING):
        registers = {k: v for k, v in self.registers.items() if (v[2] == rtype)}
        results = {}

        for batch in range(1, len(registers)):
            register_batch = {k: v for k, v in registers.items() if (v[7] == batch)}

            if not register_batch:
                break

            results.update(self._read_all(register_batch, rtype))

        return results


class Inverter(APsystems):

    def __init__(self, *args, **kwargs):
        self.model = "Inverter"
        self.wordorder = Endian.BIG

        super().__init__(*args, **kwargs)

        # fmt: off
        self.registers = {
            # name, address, length, register, type, target type, description, unit, batch
        #    "c_id": (0x9c40, 2, RegisterType.HOLDING, RegisterDataType.STRING, str, "SunSpec ID", "", 1),
            "c_did": (0x9c42, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "SunSpec DID", "", 1),
            "c_length": (0x9c43, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "SunSpec Length", "16Bit Words", 1),
            "c_manufacturer": (0x9c44, 16, RegisterType.HOLDING, RegisterDataType.STRING, str, "Manufacturer", "", 1),
            "c_model": (0x9c54, 16, RegisterType.HOLDING, RegisterDataType.STRING, str, "Model", "", 1),
            "c_version": (0x9c6c, 8, RegisterType.HOLDING, RegisterDataType.STRING, str, "Version", "", 1),
            "c_serialnumber": (0x9c74, 16, RegisterType.HOLDING, RegisterDataType.STRING, str, "Serial", "", 1),
            "c_deviceaddress": (0x9c84, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "Modbus ID", "", 1),
            "c_sunspec_did": (0x9c85, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "SunSpec DID", C_SUNSPEC_DID_MAP, 2),
            "c_sunspec_length": (0x9c86, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "Length", "16Bit Words", 2),


            "current": (0x9c88, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "Current", " ""A", 2),
            "l1_current": (0x9c89, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "L1 Current"," " "A", 2),
        #    "l2_current": (0x9c8a, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "L2 Current", "A", 2),
        #   "l3_current": (0x9c8b, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "L3 Current", "A", 2),
            "current_scale": (0x9c8c, 1, RegisterType.HOLDING, RegisterDataType.SCALE, int, "Current Scale Factor", "", 2),

        #    "l1_voltage": (0x9c8d, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "L1 Voltage"," " "V", 2),
        #   "l2_voltage": (0x9c8e, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "L2 Voltage", "V", 2),
        #   "l3_voltage": (0x9c8f, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "L3 Voltage", "V", 2),
            "l1n_voltage": (0x9c90, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "L1-N Voltage", " ""V", 2),
        #    "l2n_voltage": (0x9c91, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "L2-N Voltage", "V", 2),
        #    "l3n_voltage": (0x9c92, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "L3-N Voltage", "V", 2),
            "voltage_scale": (0x9c93, 1, RegisterType.HOLDING, RegisterDataType.SCALE, int, "Voltage Scale Factor", "", 2),

            "power_ac": (0x9c94, 1, RegisterType.HOLDING, RegisterDataType.INT16, int, "Power", " ""W", 2),
            "power_ac_scale": (0x9c95, 1, RegisterType.HOLDING, RegisterDataType.SCALE, int, "Power Scale Factor", "", 2),

            "frequency": (0x9c96, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "Frequency"," " "Hz", 2),
            "frequency_scale": (0x9c97, 1, RegisterType.HOLDING, RegisterDataType.SCALE, int, "Frequency Scale Factor", "", 2),

            "power_apparent": (0x9c98, 1, RegisterType.HOLDING, RegisterDataType.INT16, int, "Power (Apparent)"," " "VA", 2),
            "power_apparent_scale": (0x9c99, 1, RegisterType.HOLDING, RegisterDataType.SCALE, int, "Power (Apparent) Scale Factor", "", 2),
            
            "power_reactive": (0x9c9a, 1, RegisterType.HOLDING, RegisterDataType.INT16, int, "Power (Reactive)", " ""VAR" "", 2),
            "power_reactive_scale": (0x9c9b, 1, RegisterType.HOLDING, RegisterDataType.SCALE, int, "Power (Reactive) Scale Factor", "", 2),
            
            "power_factor": (0x9c9c, 1, RegisterType.HOLDING, RegisterDataType.INT16, int, "Power Factor"," " "cos φ ", 2),
            "power_factor_scale": (0x9c9d, 1, RegisterType.HOLDING, RegisterDataType.SCALE, int, "Power Factor Scale Factor", "", 2),

            "energy_total": (0x9c9e, 2, RegisterType.HOLDING, RegisterDataType.ACC32, int, "Total Energy"," " "Wh", 2),
            "energy_total_scale": (0x9ca0, 1, RegisterType.HOLDING, RegisterDataType.SCALE, int, "Total Energy Scale Factor", "", 2),

        #   "current_dc": (0x9ca1, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "DC Current", "A", 2),
        #   "current_dc_scale": (0x9ca2, 1, RegisterType.HOLDING, RegisterDataType.SCALE, int, "DC Current Scale Factor", "", 2),

        #    "voltage_dc": (0x9ca3, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "DC Voltage", "V", 2),
        #    "voltage_dc_scale": (0x9ca4, 1, RegisterType.HOLDING, RegisterDataType.SCALE, int, "DC Voltage Scale Factor", "", 2),

        #    "power_dc": (0x9ca5, 1, RegisterType.HOLDING, RegisterDataType.INT16, int, "DC Power", "W", 2),
        #    "power_dc_scale": (0x9ca6, 1, RegisterType.HOLDING, RegisterDataType.SCALE, int, "DC Power Scale Factor", "", 2),

            "temperature": (0x9ca7, 1, RegisterType.HOLDING, RegisterDataType.INT16, int, "Temperature"," " "°C", 2),
            "temperature_scale": (0x9cab, 1, RegisterType.HOLDING, RegisterDataType.SCALE, int, "Temperature Scale Factor", "", 2),

            "status": (0x9cac, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "Status", INVERTER_STATUS_MAP, 2),
        #    "vendor_status": (0x9cad, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "Vendor Status", "", 2),

        #    "rrcr_state": (0xf000, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "RRCR State", "", 3),
        #    "active_power_limit": (0xf001, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "Active Power Limit", "%", 3),
        #    "cosphi": (0xf002, 2, RegisterType.HOLDING, RegisterDataType.FLOAT32, int, "CosPhi", "", 3)
        
        }
        # fmt: off

        self.meter_dids = [
            (0x9cfc, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "", "", 1),
            (0x9daa, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "", "", 1),
        #    (0x9e59, 1, RegisterType.HOLDING, RegisterDataType.UINT16, int, "", "", 1)
        ]

    def meters(self):
        meters = [self._read(v) for v in self.meter_dids]

        return {
            f"Meter{idx + 1}": Meter(offset=idx, parent=self)
            for idx, v in enumerate(meters)
            if v
        }


class Meter(APsystems):

    def __init__(self, offset=False, *args, **kwargs):
        self.model = f"Meter{offset + 1}"
        self.wordorder = Endian.BIG

        super().__init__(*args, **kwargs)

        self.offset = METER_REGISTER_OFFSETS[offset]
        self.registers = {}
