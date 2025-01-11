import skfuzzy as fuzz
from skfuzzy import control as ctrl
import numpy as np
import matplotlib.pyplot as plt
from random import randint
import pandas as pd

sensor_data = {
    'temperature': 21,           # ideal (13-29)
    'humidity': 85,              # ideal (50-(60-80)-100)
    'lightIntensity': 86,        # ideal (70-(80-100))
    'soilMoisture1': 81,         # ideal (70-90)
    'soilMoisture2': 80,         # ideal (70-90)
    'soilMoisture3': 95         # ideal (70-90)
}

def create_fuzzy(crop_type):
    temperature_mean = ctrl.Antecedent(np.arange(0, 61, 1), 'temperature_mean')
    humidity_mean = ctrl.Antecedent(np.arange(0, 102, 1), 'humidity_mean')
    lightIntensity_mean = ctrl.Antecedent(np.arange(0, 102, 1), 'lightIntensity_mean')
    soilMoisture1_mean = ctrl.Antecedent(np.arange(0, 102, 1), 'soilMoisture1_mean')
    soilMoisture2_mean = ctrl.Antecedent(np.arange(0, 102, 1), 'soilMoisture2_mean')
    soilMoisture3_mean = ctrl.Antecedent(np.arange(0, 102, 1), 'soilMoisture3_mean')
    # Overall if 100 perfectly healthy
    overall_status = ctrl.Consequent(np.arange(0, 101, 1), 'overall_status')
    
    if crop_type.lower() == "potato":
        # Ideal Temp 13-29
        temperature_mean['not ideal low'] = fuzz.trimf(temperature_mean.universe, [0, 0, 13])
        temperature_mean['ideal'] = fuzz.trimf(temperature_mean.universe, [12, 21, 30])     
        temperature_mean['not ideal high'] = fuzz.trimf(temperature_mean.universe, [29, 60, 60])
        
        # Ideal Humid 50-(60-80)-100
        humidity_mean['not ideal low'] = fuzz.trimf(humidity_mean.universe, [0, 0, 50])
        humidity_mean['ideal'] = fuzz.trapmf(humidity_mean.universe, [49, 60, 80, 101])
        humidity_mean['not ideal high'] = fuzz.trimf(humidity_mean.universe, [100, 101, 101])

        # Ideal Light Exposure 70-(80-100)
        lightIntensity_mean['not ideal low'] = fuzz.trimf(lightIntensity_mean.universe, [0, 0, 70])
        lightIntensity_mean['ideal'] = fuzz.trapmf(lightIntensity_mean.universe, [69, 80, 100, 101])
        lightIntensity_mean['not ideal high'] = fuzz.trimf(lightIntensity_mean.universe, [100, 101, 101])

        # Ideal Soil Moisture 70-90
        soilMoisture1_mean['not ideal low'] = fuzz.trimf(soilMoisture1_mean.universe, [0, 0, 70])
        soilMoisture1_mean['ideal'] = fuzz.trapmf(soilMoisture1_mean.universe, [69, 70, 90, 91])
        soilMoisture1_mean['not ideal high'] = fuzz.trimf(soilMoisture1_mean.universe, [90, 100, 100])
        soilMoisture2_mean['not ideal low'] = fuzz.trimf(soilMoisture2_mean.universe, [0, 0, 70])
        soilMoisture2_mean['ideal'] = fuzz.trapmf(soilMoisture2_mean.universe, [69, 70, 90, 91])
        soilMoisture2_mean['not ideal high'] = fuzz.trimf(soilMoisture2_mean.universe, [90, 100, 100])
        soilMoisture3_mean['not ideal low'] = fuzz.trimf(soilMoisture3_mean.universe, [0, 0, 70])
        soilMoisture3_mean['ideal'] = fuzz.trapmf(soilMoisture3_mean.universe, [69, 70, 90, 91])
        soilMoisture3_mean['not ideal high'] = fuzz.trimf(soilMoisture3_mean.universe, [90, 100, 100])
    
    elif crop_type.lower() == 'tomato':
        # Ideal Temp 22-29
        temperature_mean['not ideal low'] = fuzz.trimf(temperature_mean.universe, [0, 0, 22])
        temperature_mean['ideal'] = fuzz.trimf(temperature_mean.universe, [21, 26, 30])     
        temperature_mean['not ideal high'] = fuzz.trimf(temperature_mean.universe, [29, 60, 60])
        
        # Ideal Humid 65-85
        humidity_mean['not ideal low'] = fuzz.trimf(humidity_mean.universe, [0, 0, 65])
        humidity_mean['ideal'] = fuzz.trapmf(humidity_mean.universe, [64, 65, 85, 86])
        humidity_mean['not ideal high'] = fuzz.trimf(humidity_mean.universe, [85, 101, 101])

        # Ideal Light Exposure 70-(80-100)
        lightIntensity_mean['not ideal low'] = fuzz.trimf(lightIntensity_mean.universe, [0, 0, 70])
        lightIntensity_mean['ideal'] = fuzz.trapmf(lightIntensity_mean.universe, [69, 80, 100, 101])
        lightIntensity_mean['not ideal high'] = fuzz.trimf(lightIntensity_mean.universe, [100, 101, 101])

        # Ideal Soil Moisture 50-70
        soilMoisture1_mean['not ideal low'] = fuzz.trimf(soilMoisture1_mean.universe, [0, 0, 50])
        soilMoisture1_mean['ideal'] = fuzz.trapmf(soilMoisture1_mean.universe, [49, 50, 70, 71])
        soilMoisture1_mean['not ideal high'] = fuzz.trimf(soilMoisture1_mean.universe, [70, 100, 100])
        soilMoisture2_mean['not ideal low'] = fuzz.trimf(soilMoisture2_mean.universe, [0, 0, 50])
        soilMoisture2_mean['ideal'] = fuzz.trapmf(soilMoisture2_mean.universe, [49, 50, 70, 71])
        soilMoisture2_mean['not ideal high'] = fuzz.trimf(soilMoisture2_mean.universe, [70, 100, 100])
        soilMoisture3_mean['not ideal low'] = fuzz.trimf(soilMoisture3_mean.universe, [0, 0, 50])
        soilMoisture3_mean['ideal'] = fuzz.trapmf(soilMoisture3_mean.universe, [49, 50, 70, 71])
        soilMoisture3_mean['not ideal high'] = fuzz.trimf(soilMoisture3_mean.universe, [70, 100, 100])
    
    elif crop_type.lower() == 'pepper':
        # Ideal Temp 16-21
        temperature_mean['not ideal low'] = fuzz.trimf(temperature_mean.universe, [0, 0, 16])
        temperature_mean['ideal'] = fuzz.trapmf(temperature_mean.universe, [15, 16, 21, 22])     
        temperature_mean['not ideal high'] = fuzz.trimf(temperature_mean.universe, [21, 60, 60])

        # Ideal Humid 65-85
        humidity_mean['not ideal low'] = fuzz.trimf(humidity_mean.universe, [0, 0, 65])
        humidity_mean['ideal'] = fuzz.trapmf(humidity_mean.universe, [64, 65, 85, 86])
        humidity_mean['not ideal high'] = fuzz.trimf(humidity_mean.universe, [85, 101, 101])

        # Ideal Light Exposure 60-100
        lightIntensity_mean['not ideal low'] = fuzz.trimf(lightIntensity_mean.universe, [0, 0, 60])
        lightIntensity_mean['ideal'] = fuzz.trapmf(lightIntensity_mean.universe, [59, 60, 100, 101])
        lightIntensity_mean['not ideal high'] = fuzz.trimf(lightIntensity_mean.universe, [100, 101, 101])

        # Ideal Soil Moisture 20-60
        soilMoisture1_mean['not ideal low'] = fuzz.trimf(soilMoisture1_mean.universe, [0, 0, 20])
        soilMoisture1_mean['ideal'] = fuzz.trapmf(soilMoisture1_mean.universe, [19, 20, 60, 61])
        soilMoisture1_mean['not ideal high'] = fuzz.trimf(soilMoisture1_mean.universe, [60, 100, 100])
        soilMoisture2_mean['not ideal low'] = fuzz.trimf(soilMoisture2_mean.universe, [0, 0, 20])
        soilMoisture2_mean['ideal'] = fuzz.trapmf(soilMoisture2_mean.universe, [19, 20, 60, 61])
        soilMoisture2_mean['not ideal high'] = fuzz.trimf(soilMoisture2_mean.universe, [60, 100, 100])
        soilMoisture3_mean['not ideal low'] = fuzz.trimf(soilMoisture3_mean.universe, [0, 0, 20])
        soilMoisture3_mean['ideal'] = fuzz.trapmf(soilMoisture3_mean.universe, [19, 20, 60, 61])
        soilMoisture3_mean['not ideal high'] = fuzz.trimf(soilMoisture3_mean.universe, [60, 100, 100])
    
    elif crop_type.lower() == 'cucumber':
        # Ideal Temp 18-35
        temperature_mean['not ideal low'] = fuzz.trimf(temperature_mean.universe, [0, 0, 15])
        temperature_mean['ideal'] = fuzz.trapmf(temperature_mean.universe, [14, 21, 26, 30])     
        temperature_mean['not ideal high'] = fuzz.trimf(temperature_mean.universe, [29, 60, 60])

        # Ideal Humid 60-85
        humidity_mean['not ideal low'] = fuzz.trimf(humidity_mean.universe, [0, 0, 60])
        humidity_mean['ideal'] = fuzz.trapmf(humidity_mean.universe, [59, 60, 85, 86])
        humidity_mean['not ideal high'] = fuzz.trimf(humidity_mean.universe, [85, 101, 101])

        # Ideal Light Exposure 60-100
        lightIntensity_mean['not ideal low'] = fuzz.trimf(lightIntensity_mean.universe, [0, 0, 70])
        lightIntensity_mean['ideal'] = fuzz.trapmf(lightIntensity_mean.universe, [69, 70, 100, 101])
        lightIntensity_mean['not ideal high'] = fuzz.trimf(lightIntensity_mean.universe, [100, 101, 101])

        # Ideal Soil Moisture 80-100
        soilMoisture1_mean['not ideal low'] = fuzz.trimf(soilMoisture1_mean.universe, [0, 0, 80])
        soilMoisture1_mean['ideal'] = fuzz.trapmf(soilMoisture1_mean.universe, [79, 80, 100, 101])
        soilMoisture1_mean['not ideal high'] = fuzz.trimf(soilMoisture1_mean.universe, [100, 101, 101])
        soilMoisture2_mean['not ideal low'] = fuzz.trimf(soilMoisture2_mean.universe, [0, 0, 80])
        soilMoisture2_mean['ideal'] = fuzz.trapmf(soilMoisture2_mean.universe, [79, 80, 100, 101])
        soilMoisture2_mean['not ideal high'] = fuzz.trimf(soilMoisture2_mean.universe, [100, 101, 101])
        soilMoisture3_mean['not ideal low'] = fuzz.trimf(soilMoisture3_mean.universe, [0, 0, 80])
        soilMoisture3_mean['ideal'] = fuzz.trapmf(soilMoisture3_mean.universe, [79, 80, 100, 101])
        soilMoisture3_mean['not ideal high'] = fuzz.trimf(soilMoisture3_mean.universe, [100, 101, 101])

    
    sensors = {
        'temperature_mean': temperature_mean,
        'humidity_mean': humidity_mean,
        'lightIntensity_mean': lightIntensity_mean,
        'soilMoisture1_mean': soilMoisture1_mean,
        'soilMoisture2_mean': soilMoisture2_mean,
        'soilMoisture3_mean': soilMoisture3_mean
    }
    display_calculated_membership(sensors)

    overall_status['unhealthy'] = fuzz.trimf(overall_status.universe, [0, 0, 50])
    overall_status['healthy'] = fuzz.trimf(overall_status.universe, [50, 100, 100])

    rule1 = ctrl.Rule(
        humidity_mean['not ideal low'] | humidity_mean['not ideal high'] |
        temperature_mean['not ideal low'] | temperature_mean['not ideal high'] |
        lightIntensity_mean['not ideal low'] | lightIntensity_mean['not ideal high'] | 
        soilMoisture1_mean['not ideal low'] | soilMoisture1_mean['not ideal high'] |
        soilMoisture2_mean['not ideal low'] | soilMoisture2_mean['not ideal high'] |
        soilMoisture3_mean['not ideal low'] | soilMoisture3_mean['not ideal high'],
        overall_status['unhealthy']
        )
    rule2 = ctrl.Rule(
        humidity_mean['ideal'] & temperature_mean['ideal'] &
        lightIntensity_mean['ideal'] & soilMoisture1_mean['ideal'] &
        soilMoisture2_mean['ideal'] & soilMoisture3_mean['ideal'], 
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


def display_calculated_membership(sensor_dict):
    data = []
    for sensor_name, sensor in sensor_dict.items():
        value_to_check = sensor_data[sensor_name.split('_')[0]]
        
        # Calculate the membership values for the specified sensor value
        low_membership = fuzz.interp_membership(sensor.universe, sensor['not ideal low'].mf, value_to_check)
        ideal_membership = fuzz.interp_membership(sensor.universe, sensor['ideal'].mf, value_to_check)
        high_membership = fuzz.interp_membership(sensor.universe, sensor['not ideal high'].mf, value_to_check)
        
        data.append({
            "Sensor": sensor_name,
            "Value": value_to_check,
            "Low": low_membership,
            "Ideal": ideal_membership,
            "High": high_membership
        })

    # Create DataFrame
    df = pd.DataFrame(data)

    # with open("sensorFuzzyDF.txt", "w") as f:
    #     f.write(df.to_string())

    print(df)

if __name__ == "__main__":
    crop_type = "cucumber"
    overall_status_simulation = create_fuzzy(crop_type)

    overall_status_simulation.input['temperature_mean'] = sensor_data['temperature']
    overall_status_simulation.input['humidity_mean'] = sensor_data['humidity']
    overall_status_simulation.input['lightIntensity_mean'] = sensor_data['lightIntensity']
    overall_status_simulation.input['soilMoisture1_mean'] = sensor_data['soilMoisture1']
    overall_status_simulation.input['soilMoisture2_mean'] = sensor_data['soilMoisture2']
    overall_status_simulation.input['soilMoisture3_mean'] = sensor_data['soilMoisture3']
    overall_status_simulation.compute()

    print(f"{crop_type.upper()} overall status: {overall_status_simulation.output['overall_status']}%")