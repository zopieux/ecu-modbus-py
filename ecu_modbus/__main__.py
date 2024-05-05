import argparse
import json

from . import apsystems_modbus


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("host", type=str, help="Modbus TCP address")
    argparser.add_argument("port", type=int, help="Modbus TCP port")
    argparser.add_argument("--timeout", type=int, default=1, help="Connection timeout")
    argparser.add_argument("--unit", type=int, default=1, help="Modbus device address")
    argparser.add_argument(
        "--json", action="store_true", default=False, help="Output as JSON"
    )
    args = argparser.parse_args()

    inverter = apsystems_modbus.Inverter(
        host=args.host, port=args.port, timeout=args.timeout, unit=args.unit
    )

    values = {}
    values = inverter.read_all()

    meters = inverter.meters()
    values["meters"] = {}

    for meter, params in meters.items():
        meter_values = params.read_all()
        values["meters"][meter] = meter_values

    if args.json:
        print(json.dumps(values, indent=4))
    else:
        print(f"{inverter}:")
        print("\nRegisters:")

        print(f"\tManufacturer: {values['c_manufacturer']}")
        print(f"\tModel: {values['c_model']}")
        #  print(f"\tType: {apsystems_modbus.C_SUNSPEC_DID_MAP[str(values['c_sunspec_did'])]}")
        print(f"\tVersion: {values['c_version']}")
        print(f"\tSerial: {values['c_serialnumber']}")
        print(f"\tStatus: {apsystems_modbus.INVERTER_STATUS_MAP[values['status']]}")
        print(
            f"\tTemperature: {(values['temperature'] *(10 ** values['temperature_scale'])):.2f}{inverter.registers['temperature'][6]}"
        )

        print(
            f"\tCurrent: {(values['current'] * (10 ** values['temperature_scale'])/10):.2f}{inverter.registers['current'][6]}"
        )

        print(
            f"\tVoltage: {(values['l1n_voltage'] * (10 ** values['voltage_scale'])):.2f}{inverter.registers['l1n_voltage'][6]}"
        )

        print(
            f"\tFrequency: {(values['frequency'] * (10 ** values['frequency_scale'])):.3f}{inverter.registers['frequency'][6]}"
        )
        print(
            f"\tPower: {(values['power_ac'] * (10 ** values['power_ac_scale'])):.2f}{inverter.registers['power_ac'][6]}"
        )
        print(
            f"\tPower (Apparent): {(values['power_apparent'] * (10 ** values['power_apparent_scale'])):.2f}{inverter.registers['power_apparent'][6]}"
        )
        print(
            f"\tPower (Reactive): {(values['power_reactive'] * (10 ** values['power_reactive_scale'])):.2f}{inverter.registers['power_reactive'][6]}"
        )
        print(
            f"\tPower Factor: {(values['power_factor'] * (10 ** values['power_factor_scale'])):.3f}{inverter.registers['power_factor'][6]}"
        )
        print(
            f"\tTotal Energy: {(values['energy_total'] * (10 ** values['energy_total_scale']))}{inverter.registers['energy_total'][6]}"
        )
