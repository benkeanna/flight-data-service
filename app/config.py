import os

from dotenv import load_dotenv
load_dotenv()

class Config:
    FFA_DATA_DIR_PATH = os.path.join(os.path.dirname(__file__), 'data')
