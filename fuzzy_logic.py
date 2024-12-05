import numpy as np
import skfuzzy as fuzz
import json


def notification_plant_status(plant_status):
    # Convert into (2) decimal places
    for key in plant_status:
        plant_status[key] = [round(value, 2) for value in plant_status[key]]

    # Use if else here to return notification


# Temperature ranges
temp = np.arange(0, 40, 1)
temp_low = fuzz.trimf(temp, [0, 0, 18])
temp_optimal = fuzz.trimf(temp, [17, 22, 26])
temp_high = fuzz.trimf(temp, [25, 39, 39])

# Humidity ranges
humidity = np.arange(0, 100, 1)
humidity_low = fuzz.trimf(humidity, [0, 0, 60])
humidity_optimal = fuzz.trimf(humidity, [59, 70, 81])
humidity_high = fuzz.trimf(humidity, [80, 99, 99])

# Soil moisture ranges
soil_moisture = np.arange(0, 100, 1)
soil_moisture_low = fuzz.trimf(soil_moisture, [0, 0, 30])
soil_moisture_optimal = fuzz.trimf(soil_moisture, [29, 50, 71])
soil_moisture_high = fuzz.trimf(soil_moisture, [70, 90, 99])

# Light ranges for fuzzy logic (hours per day)
light = np.arange(0, 20, 1)
light_low = fuzz.trimf(light, [0, 2, 6])
light_optimal = fuzz.trimf(light, [5, 10, 13])
light_high = fuzz.trimf(light, [12, 19, 19])

# Define output variables for the status of the plant
plant_status = np.arange(0, 10, 1)
status_critical = fuzz.trimf(plant_status, [0, 0, 4])    # [1, 0,66, 0,33, 0,   0,   0,   0,  0,    0,    0]
status_warning = fuzz.trimf(plant_status, [3, 5, 7])     # [0, 0,    0,    0.5, 1,   0.5, 0,  0,    0,    0]
status_healthy = fuzz.trimf(plant_status, [6, 9, 9])     # [0, 0,    0     0    0,   0,   0,  0.33, 0.66, 1]     


# Example sensor data (replace with real-time data)
sensor_data = {
    "temperature": 17.0,    # optimal [18, 22, 25]
    "humidity": 70.0,       # optimal [60, 70, 80]
    "soil_moisture": 22.0,   # optimal [30, 50, 70]
    "light": 10.0           # optimal [6, 10, 12]
}

# Fuzzify input data
temp_level_low = fuzz.interp_membership(temp, temp_low, sensor_data["temperature"])
temp_level_optimal = fuzz.interp_membership(temp, temp_optimal, sensor_data["temperature"])
temp_level_high = fuzz.interp_membership(temp, temp_high, sensor_data["temperature"])

humidity_level_low = fuzz.interp_membership(humidity, humidity_low, sensor_data["humidity"])
humidity_level_optimal = fuzz.interp_membership(humidity, humidity_optimal, sensor_data["humidity"])
humidity_level_high = fuzz.interp_membership(humidity, humidity_high, sensor_data["humidity"])

soil_level_low = fuzz.interp_membership(soil_moisture, soil_moisture_low, sensor_data["soil_moisture"])
soil_level_optimal = fuzz.interp_membership(soil_moisture, soil_moisture_optimal, sensor_data["soil_moisture"])
soil_level_high = fuzz.interp_membership(soil_moisture, soil_moisture_high, sensor_data["soil_moisture"])

light_level_low = fuzz.interp_membership(light, light_low, sensor_data["light"])
light_level_optimal = fuzz.interp_membership(light, light_optimal, sensor_data["light"])
light_level_high = fuzz.interp_membership(light, light_high, sensor_data["light"])

# Convert fuzzy membership values to standard Python floats or lists
notif_plant_status = {
    'temp': [float(temp_level_low), float(temp_level_optimal), float(temp_level_high)],
    'humidity': [float(humidity_level_low), float(humidity_level_optimal), float(humidity_level_high)],
    'soil': [float(soil_level_low), float(soil_level_optimal), float(soil_level_high)],
    'light': [float(light_level_low), float(light_level_optimal), float(light_level_high)]
}

# Set notification
notification_plant_status(notif_plant_status)

# Troubleshoot purpose (display output)
# print("\nTemperature low membership:", temp_level_low)
# print("Temperature optimal membership:", temp_level_optimal)
# print("Temperature high membership:", temp_level_high)

# print("\nHumidity low membership:", humidity_level_low)
# print("Humidity optimal membership:", humidity_level_optimal)
# print("Humidity high membership:", humidity_level_high)

# print("\nSoil Level low membership:", soil_level_low)
# print("Soil Level optimal membership:", soil_level_optimal)
# print("Soil Level high membership:", soil_level_high)

# print("\nLight low membership:", light_level_low)
# print("Light optimal membership:", light_level_optimal)
# print("Light high membership:", light_level_high)

# Fuzzy rule application (if-else for basic rules)
# Use max() for combining the fuzzy sets to give a final plant status.

# Plant status rules
rule_temp_low = np.fmin(temp_level_low, status_critical)
rule_temp_optimal = np.fmin(temp_level_optimal, status_healthy)
rule_temp_high = np.fmin(temp_level_high, status_critical)

rule_humidity_low = np.fmin(humidity_level_low, status_warning)
rule_humidity_optimal = np.fmin(humidity_level_optimal, status_healthy)
rule_humidity_high = np.fmin(humidity_level_high, status_critical)

rule_light_low = np.fmin(light_level_low, status_warning)
rule_light_optimal = np.fmin(light_level_optimal, status_healthy)
rule_light_high = np.fmin(light_level_high, status_critical)

rule_soil_moisture_low = np.fmin(soil_level_low, status_critical)
rule_soil_moisture_optimal = np.fmin(soil_level_optimal, status_healthy)
rule_soil_moisture_high = np.fmin(soil_level_high, status_warning)


# Combine rules step-by-step
combined_temp = np.fmax(rule_temp_low, np.fmax(rule_temp_optimal, rule_temp_high))
combined_humidity = np.fmax(rule_humidity_low, np.fmax(rule_humidity_optimal, rule_humidity_high))
combined_soil_moisture = np.fmax(rule_soil_moisture_low, np.fmax(rule_soil_moisture_optimal, rule_soil_moisture_high))
combined_light = np.fmax(rule_light_low, np.fmax(rule_light_optimal, rule_light_high))
plant_condition = np.fmax(combined_temp, np.fmax(combined_humidity, np.fmax(combined_soil_moisture, combined_light)))


print(plant_condition)


# Defuzzification (converting fuzzy result to crisp value)
final_plant_status = fuzz.defuzz(plant_status, plant_condition, 'centroid')


# Output the final plant status
if final_plant_status <= 5:
    plant_condition_str = "Critical"
elif final_plant_status <= 7:
    plant_condition_str = "Warning"
else:
    plant_condition_str = "Healthy"

print(f"Decimal: {final_plant_status} | Status: {plant_condition_str}")
