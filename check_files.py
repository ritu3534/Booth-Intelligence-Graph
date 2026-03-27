import os
from dotenv import find_dotenv

print(f"Current Working Directory: {os.getcwd()}")
print(f"Files in this folder: {os.listdir('.')}")
print(f"Files in parent folder: {os.listdir('..')}")
print(f"Find_dotenv found: {find_dotenv()}")