import pandas as pd
import sys

def verify_jansetu_csv(file_path, expected_cols):
    print(f"\n🔍 Checking: {file_path}")
    try:
        # Read only the first row to save memory
        df = pd.read_csv(file_path, nrows=0)
        actual_cols = [c.strip() for c in df.columns.tolist()]
        
        print(f"✅ Found headers: {actual_cols}")
        
        missing = [col for col in expected_cols if col not in actual_cols]
        if missing:
            print(f"❌ ERROR: Missing columns: {missing}")
            print(f"👉 Your CSV must have exactly these headers: {expected_cols}")
        else:
            print("🟢 SUCCESS: All required columns are present and stripped of whitespace.")
            
    except Exception as e:
        print(f"❌ Failed to read file: {e}")

# Define the requirements based on your graph_engine.py logic
VOTER_EXPECTED = ['booth_no', 'voter_id', 'name', 'age', 'occupation', 'income_bracket', 'phone']
SCHEME_EXPECTED = ['voter_id', 'scheme_name', 'category']

if __name__ == "__main__":
    # Update these paths to where your actual CSVs are stored
    verify_jansetu_csv("voter_list.csv", VOTER_EXPECTED)
    verify_jansetu_csv("scheme_list.csv", SCHEME_EXPECTED)