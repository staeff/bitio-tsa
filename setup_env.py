from dotenv import load_dotenv
from getpass import getpass
from pathlib import Path

# write a .env file if you don't have one already
p = Path('.')
if not (p / '.env').exists():
    with open(p / '.env', 'w') as d:
        d.write(f'PG_STRING = {getpass("Please Enter your Postgresql Connection String")}\n')
        d.write(f'USERNAME = {getpass("Please Enter your bit.io Username")}\n')
        d.write(f'BITIO_KEY = {getpass("Please Enter your bit.io API Key")}\n')
else:
    print("Detected existing .env file.")
    load_dotenv()

    # Check if the correct vars exist in the .env and add if necessary
    if os.getenv('PG_STRING'):
        print("Postgresql Connection String Detected in .env file")
    else:
        with open(p / '.env', 'a') as d:
            d.write(f'PG_STRING = {getpass("Please Enter your Postgresql Connection String")}\n')
            print('Wrote Postgresql connection string to .env')

    if os.getenv('USERNAME'):
        print("bit.io Username detected in .env file")
    else:
        with open(p / '.env', 'a') as d:
            d.write(f'USERNAME = {getpass("Please Enter your bit.io Username")}\n')
            print('Wrote bit.io Username to .env file')

    if os.getenv('BITIO_KEY'):
        print("bit.io api key detected in .env file")
    else:
        with open(p / '.env', 'a') as d:
            d.write(f'BITIO_KEY = {getpass("Please Enter your bit.io API Key")}\n')
            print('Wrote bit.io API Key to .env file')

# Load the final dotenv.
load_dotenv()
