from unittest.mock import patch
import pytest
from anue_wordscloud import get_api_key, call_chagpt


def test_get_api_key():
    with patch('decouple.config') as mock_config:
        mock_config.return_value = "test_api_key"
        assert get_api_key() == "test_api_key"


def test_call_chagpt():
    with patch('openai.ChatCompletion.create') as mock_create:
        mock_create.return_value = {
            'choices': [{'message': {'content': 'test_content'}}]
        }
        result = call_chagpt("test_prompt")
        assert result == "test_content"

