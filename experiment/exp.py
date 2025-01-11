import skfuzzy as fuzz
from skfuzzy import control as ctrl
import numpy as np
import matplotlib.pyplot as plt
from random import randint

sensor_data = {
    'temperature': 21,   # ideal (13-29)
    'humidity': 50,      # ideal (50-100)
    'lightIntensity': 89,  
    'soilMoisture1': 40, 
    'soilMoisture2': 45,  
    'soilMoisture3': 50 
}

def create_fuzzy():
    temperature_mean = ctrl.Antecedent(np.arange(0, 61, 1), 'temperature_mean')
    humidity_mean = ctrl.Antecedent(np.arange(0, 102, 1), 'humidity_mean')
    lightIntensity_mean = ctrl.Antecedent(np.arange(0, 101, 1), 'lightIntensity_mean')
    soilMoisture1_mean = ctrl.Antecedent(np.arange(0, 101, 1), 'soilMoisture1_mean')
    soilMoisture2_mean = ctrl.Antecedent(np.arange(0, 101, 1), 'soilMoisture2_mean')
    soilMoisture3_mean = ctrl.Antecedent(np.arange(0, 101, 1), 'soilMoisture3_mean')
    # Overall if 100 perfectly healthy
    overall_status = ctrl.Consequent(np.arange(0, 101, 1), 'overall_status')

    # POTATO
    # Ideal Temp 13 to 29
    temperature_mean['not ideal low'] = fuzz.trimf(temperature_mean.universe, [0, 0, 13])
    temperature_mean['ideal'] = fuzz.trimf(temperature_mean.universe, [12, 21, 30])     
    temperature_mean['not ideal high'] = fuzz.trimf(temperature_mean.universe, [29, 60, 60])
    
    # Ideal Humid 50 to 100 (Ideal 60 - 80)
    humidity_mean['not ideal low'] = fuzz.trimf(humidity_mean.universe, [0, 0, 50])
    humidity_mean['ideal'] = fuzz.trapmf(humidity_mean.universe, [49, 60, 80, 101])
    humidity_mean['not ideal high'] = fuzz.trimf(humidity_mean.universe, [100, 101, 101])

    overall_status['unhealthy'] = fuzz.trimf(overall_status.universe, [0, 0, 50])
    overall_status['healthy'] = fuzz.trimf(overall_status.universe, [50, 100, 100])

    rule1 = ctrl.Rule(
        humidity_mean['not ideal low'] | humidity_mean['not ideal high'] |
        temperature_mean['not ideal low'] | temperature_mean['not ideal high'], 
        overall_status['unhealthy']
        )
    rule2 = ctrl.Rule(
        humidity_mean['ideal'] | temperature_mean['ideal'],
        overall_status['healthy']
        )

    overall_status_ctrl = ctrl.ControlSystem([rule1, rule2])
    return ctrl.ControlSystemSimulation(overall_status_ctrl)



def display_trapezoidal_membership(sensor):
    # Plot all defined membership functions
    plt.figure(figsize=(10, 6))
    for term_name, membership_function in sensor.terms.items():
        plt.plot(sensor.universe, membership_function.mf, label=term_name)

    # Add titles and legend
    plt.title(f'Membership Functions for {sensor.label}')
    plt.xlabel('Temperature (°C)')
    plt.ylabel('Membership Degree')
    plt.legend()
    plt.grid()
    plt.show()


def display_calculated_membership(sensor_data, what_sensor):
    value_to_check = sensor_data[what_sensor]
    
    # Calculate the membership values for the specified temperature value
    low_membership = fuzz.interp_membership(sensor_data.universe, sensor_data['not ideal low'].mf, value_to_check)
    ideal_membership = fuzz.interp_membership(sensor_data.universe, sensor_data['ideal'].mf, value_to_check)
    high_membership = fuzz.interp_membership(sensor_data.universe, sensor_data['not ideal high'].mf, value_to_check)
    
    print(value_to_check)
    print(low_membership)
    print(ideal_membership)
    print(high_membership)

    # # Highlight the specific value on the plot
    # plt.figure(figsize=(10, 6))
    # plt.plot(value_to_check, low_membership, 'ro', label=f'Value {value_to_check}°C (Not Ideal Low)')
    # plt.plot(value_to_check, ideal_membership, 'go', label=f'Value {value_to_check}°C (Ideal)')
    # plt.plot(value_to_check, high_membership, 'bo', label=f'Value {value_to_check}°C (Not Ideal High)')

    # plt.legend(loc='upper right')
    # plt.show()

if __name__ == "__main__":
    overall_status_simulation = create_fuzzy()

    overall_status_simulation.input['temperature_mean'] = sensor_data['temperature']
    overall_status_simulation.input['humidity_mean'] = sensor_data['humidity']
    overall_status_simulation.compute()

    print(overall_status_simulation.output['overall_status'])