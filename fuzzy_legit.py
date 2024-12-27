import matplotlib.pyplot as plt
from skfuzzy import control as ctrl
import skfuzzy as fuzz
import numpy as np
import pandas as pd
import random

def create_mock_data():
    data = {
        'temperature': random.randint(0, 50),
        'humidity': random.randint(0, 100),
        'lightIntensity': random.randint(0, 65536),
        'soilMoisture': random.randint(0, 100)
    }
    
    return data


def create_fuzzy_system():
    # Define fuzzy variables
    temperature = ctrl.Antecedent(np.arange(0, 51, 1), 'temperature')
    humidity = ctrl.Antecedent(np.arange(0, 101, 1), 'humidity')
    lightIntensity = ctrl.Antecedent(np.arange(0, 65536, 1), 'lightIntensity')
    soilMoisture = ctrl.Antecedent(np.arange(0, 101, 1), 'soilMoisture')
    overall_status = ctrl.Consequent(np.arange(0, 101, 1), 'overall_status')

    # Define fuzzy membership functions
    temperature['low'] = fuzz.trimf(temperature.universe, [0, 0, 20])
    temperature['ideal'] = fuzz.trimf(temperature.universe, [18, 21, 25])
    temperature['high'] = fuzz.trimf(temperature.universe, [23, 50, 50])

    humidity['low'] = fuzz.trimf(humidity.universe, [0, 0, 30])
    humidity['ideal'] = fuzz.trimf(humidity.universe, [30, 45, 60])
    humidity['high'] = fuzz.trimf(humidity.universe, [60, 100, 100])

    lightIntensity['low'] = fuzz.trimf(lightIntensity.universe, [0, 0, 20000])
    lightIntensity['ideal'] = fuzz.trimf(lightIntensity.universe, [20000, 35000, 60000])
    lightIntensity['high'] = fuzz.trimf(lightIntensity.universe, [50000, 65537, 65537])

    soilMoisture['low'] = fuzz.trimf(soilMoisture.universe, [0, 0, 30])
    soilMoisture['ideal'] = fuzz.trimf(soilMoisture.universe, [40, 60, 80])
    soilMoisture['high'] = fuzz.trimf(soilMoisture.universe, [70, 100, 100])

    overall_status['unhealthy'] = fuzz.trimf(overall_status.universe, [0, 0, 50])
    overall_status['healthy'] = fuzz.trimf(overall_status.universe, [50, 100, 100])

    # Define fuzzy rules (AND/OR)
    rule1 = ctrl.Rule(temperature['low'] | humidity['low'] | lightIntensity['low'] | soilMoisture['low'], overall_status['unhealthy'])
    rule2 = ctrl.Rule(temperature['ideal'] | humidity['ideal'] | lightIntensity['ideal'] | soilMoisture['ideal'], overall_status['healthy'])
    rule3 = ctrl.Rule(temperature['high'] | humidity['high'] | lightIntensity['high'] | soilMoisture['high'], overall_status['unhealthy'])
    
    overall_status_ctrl = ctrl.ControlSystem([rule1, rule2, rule3])
    
    return ctrl.ControlSystemSimulation(overall_status_ctrl)


def solve_fuzzy_system(fuzzy_system, data):
    fuzzy_system.input['temperature'] = data['temperature']
    fuzzy_system.input['humidity'] = data['humidity']
    fuzzy_system.input['lightIntensity'] = data['lightIntensity']
    fuzzy_system.input['soilMoisture'] = data['soilMoisture']
    fuzzy_system.compute()
    print(f"Computed output: {fuzzy_system.output}")  # Debugging print
    if 'overall_status' in fuzzy_system.output:
        return float(fuzzy_system.output['overall_status'])
    else:
        print("Error: 'overall_status' not found in fuzzy system output")
        return None


# Create mock data
mock_data = create_mock_data()

# Create fuzzy system
fuzzy_system = create_fuzzy_system()

# Solve fuzzy system for mock data
mock_data['overall_status'] = solve_fuzzy_system(fuzzy_system, mock_data)

print(mock_data)

