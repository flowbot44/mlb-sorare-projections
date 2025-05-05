import unittest
import sys
import os
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
import pytz
import requests
import json

# Add the parent directory to sys.path to import the module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from grok_ballpark_factor import (
    get_weather_nws,
    wind_dir_to_degrees,
    get_wind_effect,
    get_wind_effect_label,
    get_temp_adjustment
)


class TestWindDirectionFunctions(unittest.TestCase):
    """Test the wind direction related functions"""

    def test_wind_dir_to_degrees(self):
        """Test conversion of cardinal directions to degrees"""
        # Test cardinal directions
        self.assertEqual(wind_dir_to_degrees('N'), 0)
        self.assertEqual(wind_dir_to_degrees('E'), 90)
        self.assertEqual(wind_dir_to_degrees('S'), 180)
        self.assertEqual(wind_dir_to_degrees('W'), 270)
        
        # Test intercardinal directions
        self.assertEqual(wind_dir_to_degrees('NE'), 45)
        self.assertEqual(wind_dir_to_degrees('SE'), 135)
        self.assertEqual(wind_dir_to_degrees('SW'), 225)
        self.assertEqual(wind_dir_to_degrees('NW'), 315)
        
        # Test secondary intercardinal directions
        self.assertEqual(wind_dir_to_degrees('NNE'), 22.5)
        self.assertEqual(wind_dir_to_degrees('ENE'), 67.5)
        self.assertEqual(wind_dir_to_degrees('ESE'), 112.5)
        self.assertEqual(wind_dir_to_degrees('SSE'), 157.5)
        self.assertEqual(wind_dir_to_degrees('SSW'), 202.5)
        self.assertEqual(wind_dir_to_degrees('WSW'), 247.5)
        self.assertEqual(wind_dir_to_degrees('WNW'), 292.5)
        self.assertEqual(wind_dir_to_degrees('NNW'), 337.5)
        
        # Test case insensitivity
        self.assertEqual(wind_dir_to_degrees('ne'), 45)
        self.assertEqual(wind_dir_to_degrees('Sw'), 225)
        
        # Test default value for None or invalid direction
        self.assertEqual(wind_dir_to_degrees(None), 0)
        self.assertEqual(wind_dir_to_degrees('INVALID'), 0)


class TestWindEffectFunctions(unittest.TestCase):
    """Test the wind effect calculation functions"""

    def test_get_wind_effect(self):
        """Test calculation of wind effect"""
        # Test outward wind (tailwind) - should increase scoring
        self.assertEqual(get_wind_effect(0, 180, 15), 1.1)  # Stadium facing N, wind from S, strong
        self.assertEqual(get_wind_effect(90, 270, 15), 1.1)  # Stadium facing E, wind from W, strong
        
        # Test inward wind (headwind) - should decrease scoring
        self.assertEqual(get_wind_effect(0, 0, 15), 0.9)    # Stadium facing N, wind from N, strong
        self.assertEqual(get_wind_effect(90, 90, 15), 0.9)  # Stadium facing E, wind from E, strong
        
        # Test crosswind - should be neutral
        self.assertEqual(get_wind_effect(0, 90, 15), 1.0)   # Stadium facing N, wind from E, strong
        self.assertEqual(get_wind_effect(0, 270, 15), 1.0)  # Stadium facing N, wind from W, strong
        
        # Test wind angle near the thresholds
        self.assertEqual(get_wind_effect(0, 44, 15), 0.9)   # Just within 45° of headwind
        self.assertEqual(get_wind_effect(0, 46, 15), 1.0)   # Just outside 45° of headwind
        self.assertEqual(get_wind_effect(0, 134, 15), 1.0)  # Just outside 45° of tailwind
        self.assertEqual(get_wind_effect(0, 136, 15), 1.1)  # Just within 45° of tailwind
        
        # Test weak wind speed - should be neutral regardless of direction
        self.assertEqual(get_wind_effect(0, 0, 5), 1.0)     # Headwind but weak
        self.assertEqual(get_wind_effect(0, 180, 5), 1.0)   # Tailwind but weak
        
        # Test borderline wind speed
        self.assertEqual(get_wind_effect(0, 0, 10), 1.0)    # Borderline wind speed, should be neutral
        self.assertEqual(get_wind_effect(0, 0, 11), 0.9)    # Just above threshold, should decrease

    def test_get_wind_effect_label(self):
        """Test labeling of wind effect"""
        # Test wind direction labels
        self.assertEqual(get_wind_effect_label(0, 0), "In")       # Stadium facing N, wind from N = inward
        self.assertEqual(get_wind_effect_label(0, 180), "Out")    # Stadium facing N, wind from S = outward
        self.assertEqual(get_wind_effect_label(0, 90), "Cross")   # Stadium facing N, wind from E = crosswind
        self.assertEqual(get_wind_effect_label(0, 270), "Cross")  # Stadium facing N, wind from W = crosswind
        
        # Test angles at boundaries
        self.assertEqual(get_wind_effect_label(0, 44), "In")      # Just within "In" threshold
        self.assertEqual(get_wind_effect_label(0, 46), "Cross")   # Just outside "In" threshold
        self.assertEqual(get_wind_effect_label(0, 134), "Cross")  # Just outside "Out" threshold
        self.assertEqual(get_wind_effect_label(0, 136), "Out")    # Just within "Out" threshold
        
        # Test with None values
        self.assertEqual(get_wind_effect_label(None, 0), "Neutral")
        self.assertEqual(get_wind_effect_label(0, None), "Neutral")
        self.assertEqual(get_wind_effect_label(None, None), "Neutral")


class TestTemperatureFunction(unittest.TestCase):
    """Test the temperature adjustment function"""

    def test_get_temp_adjustment(self):
        """Test temperature adjustments to home run factor"""
        # Test cold temperature - should decrease scoring
        self.assertEqual(get_temp_adjustment(40), 0.95)
        self.assertEqual(get_temp_adjustment(59), 0.95)
        
        # Test moderate temperature - should be neutral
        self.assertEqual(get_temp_adjustment(60), 1.0)
        self.assertEqual(get_temp_adjustment(70), 1.0)
        self.assertEqual(get_temp_adjustment(80), 1.0)
        
        # Test hot temperature - should increase scoring
        self.assertEqual(get_temp_adjustment(81), 1.05)
        self.assertEqual(get_temp_adjustment(90), 1.05)
        
        # Test boundary values
        self.assertEqual(get_temp_adjustment(59.9), 0.95)
        self.assertEqual(get_temp_adjustment(60.0), 1.0)
        self.assertEqual(get_temp_adjustment(80.0), 1.0)
        self.assertEqual(get_temp_adjustment(80.1), 1.05)


class TestWeatherApiFunction(unittest.TestCase):
    """Test the NWS weather API function"""

    @patch('requests.get')
    def test_get_weather_nws_success(self, mock_get):
        """Test successful API call to NWS"""
        # Setup mock responses for the two API calls
        points_response = MagicMock()
        points_response.raise_for_status.return_value = None
        points_response.json.return_value = {
            'properties': {
                'forecastHourly': 'https://api.weather.gov/gridpoints/XXX/YY/ZZ/forecast/hourly'
            }
        }
        
        forecast_response = MagicMock()
        forecast_response.raise_for_status.return_value = None
        forecast_time = datetime.now(pytz.utc)
        forecast_response.json.return_value = {
            'properties': {
                'periods': [
                    {
                        'startTime': (forecast_time - timedelta(hours=1)).isoformat(),
                        'endTime': (forecast_time + timedelta(hours=1)).isoformat(),
                        'temperature': 75,
                        'windSpeed': '10 mph',
                        'windDirection': 'NE',
                        'probabilityOfPrecipitation': {'value': 20}
                    }
                ]
            }
        }
        
        # Configure the mock to return the correct response for each call
        mock_get.side_effect = [points_response, forecast_response]
        
        # Call the function
        result = get_weather_nws(40.7128, -74.0060, forecast_time)
        
        # Verify the result
        self.assertIsNotNone(result)
        self.assertEqual(result['temp'], 75)
        self.assertEqual(result['wind_speed'], 10)
        self.assertEqual(result['wind_dir'], 45)  # NE = 45 degrees
        self.assertEqual(result['rain'], 20)
        
        # Verify the API calls were made correctly
        mock_get.assert_any_call('https://api.weather.gov/points/40.7128,-74.006')
        mock_get.assert_any_call('https://api.weather.gov/gridpoints/XXX/YY/ZZ/forecast/hourly')

    @patch('requests.get')
    def test_get_weather_nws_invalid_coordinates(self, mock_get):
        """Test with invalid coordinates"""
        # Call with invalid latitude
        result = get_weather_nws(100, -74.0060, datetime.now(pytz.utc))
        self.assertIsNone(result)
        
        # Call with invalid longitude
        result = get_weather_nws(40.7128, -200, datetime.now(pytz.utc))
        self.assertIsNone(result)
        
        # Verify no API calls were made
        mock_get.assert_not_called()

    @patch('requests.get')
    def test_get_weather_nws_api_error(self, mock_get):
        """Test handling of API errors"""
        # Setup mock to raise an exception
        mock_get.side_effect = requests.exceptions.RequestException("API error")
        
        # Call the function and verify it handles the error gracefully
        result = get_weather_nws(40.7128, -74.0060, datetime.now(pytz.utc))
        self.assertIsNone(result)

    @patch('requests.get')
    def test_get_weather_nws_missing_data(self, mock_get):
        """Test handling of missing data in API response"""
        # Setup mock response with missing data
        points_response = MagicMock()
        points_response.raise_for_status.return_value = None
        points_response.json.return_value = {'properties': {}}  # Missing forecastHourly
        
        mock_get.return_value = points_response
        
        # Call the function and verify it handles the missing data gracefully
        result = get_weather_nws(40.7128, -74.0060, datetime.now(pytz.utc))
        self.assertIsNone(result)

    @patch('requests.get')
    def test_get_weather_nws_no_matching_period(self, mock_get):
        """Test when no forecast period matches the requested time"""
        # Setup mock responses
        points_response = MagicMock()
        points_response.raise_for_status.return_value = None
        points_response.json.return_value = {
            'properties': {
                'forecastHourly': 'https://api.weather.gov/gridpoints/XXX/YY/ZZ/forecast/hourly'
            }
        }
        
        forecast_response = MagicMock()
        forecast_response.raise_for_status.return_value = None
        forecast_time = datetime.now(pytz.utc)
        # Create periods that don't include the forecast time
        forecast_response.json.return_value = {
            'properties': {
                'periods': [
                    {
                        'startTime': (forecast_time + timedelta(hours=2)).isoformat(),
                        'endTime': (forecast_time + timedelta(hours=3)).isoformat(),
                        'temperature': 75,
                        'windSpeed': '10 mph',
                        'windDirection': 'NE',
                        'probabilityOfPrecipitation': {'value': 20}
                    }
                ]
            }
        }
        
        # Configure the mock
        mock_get.side_effect = [points_response, forecast_response]
        
        # Call the function and verify it handles this gracefully
        result = get_weather_nws(40.7128, -74.0060, forecast_time)
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main() 