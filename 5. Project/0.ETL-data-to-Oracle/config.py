import os

# Load environment variables
from dotenv import load_dotenv
load_dotenv('.env')

class AppConfig:


    oracle_config = {
        'host':  os.getenv('ORACLE_HOST'),
        'user': os.getenv('ORACLE_USER'),
        'pwd': os.getenv('ORACLE_PASS'),
        'port': os.getenv('ORACLE_PORT'),
        'ip': os.getenv('ORACLE_IP'),
        'service_name': os.getenv('ORACLE_SERVICE_NAME')
    }

config = AppConfig()


# config.oracle_config["host"]

# print(config.oracle_config.get("host", "unknown"))